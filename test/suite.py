

import doctest
from enum import Enum
from importlib import import_module
from importlib.util import spec_from_file_location
import inspect
import os
import re
import sys
import traceback


def split_path(path):
    parts = []
    while path:
        path, tail = os.path.split(path)
        parts.insert(0, tail)
    return parts


def find_package_module_names(package):
    if isinstance(package, str):
        package = import_module(package)
    root = os.path.dirname(package.__file__)
    for base, dirs, files in os.walk(root):
        if '__pycache__' in dirs:
            dirs.remove('__pycache__')
        for filename in sorted(files):
            if filename.endswith('.py'):
                parts = split_path(os.path.relpath(
                    os.path.join(base, filename[:-3]), root))
                if parts[-1] == '__init__':
                    del parts[-1]
                parts.insert(0, package.__name__)
                yield '.'.join(parts)


def format_exception(exception):
    return ''.join(traceback.format_exception(
        exception.__class__, exception, exception.__traceback__)).rstrip()


class Context(object):
    def __init__(self, file, line, module, name, source_block,
                 source_line=None):
        self.file = file
        self.line = line
        self.module = module
        self.name = name
        self.source_line = source_line
        self.source_block = source_block

    @classmethod
    def module_from_code(cls, code):
        for module in sys.modules.values():
            if getattr(module, '__file__', None) == code.co_filename:
                return module
        else:
            return None

    @classmethod
    def from_code(cls, code, line=None):
        line = line or code.co_firstlineno
        try:
            source_line, source_block = cls.get_source_lines(code, line)
        except IndexError:
            source_line, source_block = None, None
        self = cls(
            file=code.co_filename,
            module=cls.module_from_code(code).__name__,
            line=line or code.co_firstlineno,
            name=code.co_name,
            source_block=source_block,
            source_line=source_line,
        )
        self.code = code
        return self

    @classmethod
    def from_frame(cls, frame):
        self = cls.from_code(frame.f_code, frame.f_lineno)
        self.frame = frame
        self.locals = frame.f_locals
        return self

    @classmethod
    def from_traceback(cls, tb):
        while tb.tb_next:
            tb = tb.tb_next
        self = cls.from_frame(tb.tb_frame)
        self.traceback = tb
        return self

    @classmethod
    def from_callable(cls, class_or_func):
        name = None
        try:
            code = class_or_func.__code__
        except AttributeError:
            name = class_or_func.__name__
            try:
                code = class_or_func.__call__.__code__
            except AttributeError:
                code = class_or_func.__init__.__code__
        self = cls.from_code(code)
        if name:
            self.name = name
        return self

    @classmethod
    def get_source_lines(cls, obj, line):
        # Find all lines in the same block and trim indentation
        source, first_line = inspect.getsourcelines(obj)
        index_lineno = line - first_line
        index_line = source[index_lineno]
        indentation = re.match(r"^(\s*)", index_line).group(1)
        same_level = [i for i, line in enumerate(source)
                      if line.startswith(indentation)]
        begin = index_lineno
        while begin - 1 in same_level:
            begin -= 1
        end = index_lineno
        while end in same_level:
            end += 1
        lines = [line[len(indentation):] for line in source[begin:end]]
        return (index_line,
                ''.join(lines).rstrip())


class ResultStatus(Enum):
    Success = 1
    Failure = 2
    Error = 4


Success = ResultStatus.Success
Failure = ResultStatus.Failure
Error = ResultStatus.Error
Unsuccessful = (Failure, Error)


class TestResult(object):
    def __init__(self, status, context, expected, actual):
        self.file = os.path.relpath(context.file)
        self.line = context.line
        self.module = context.module
        self.function = context.name
        self.name = ('{}.{}'.format(context.module, context.name)
                     if context.name else context.module)
        self.source = context.source_block
        self.source_block = context.source_block
        self.source_line = context.source_line and context.source_line.strip()
        self.expected = expected
        self.actual = actual
        self.status = status

    @classmethod
    def failed_assertion(cls, value, tb):
        expected = None
        info = Context.from_traceback(tb)
        actual = "\n    ".join((
            value.args[0] if value.args else '',
            ", ".join("{}={!r}".format(key, value) for key, value
                      in info.locals.items() if key in info.source_line),
        ))
        return cls(Failure, info, expected, actual)

    @classmethod
    def trace_current_frame(cls, status, expected, actual=None):
        frame = inspect.currentframe()
        while frame.f_code.co_filename == __file__:
            frame = frame.f_back
        return cls(status, Context.from_frame(frame), expected, actual)

    @classmethod
    def failed_to_raise(cls, exception, actual):
        return cls.trace_current_frame(Failure, exception, actual)

    @classmethod
    def error_from_exception(cls, exception):
        info = Context.from_traceback(exception.__traceback__)
        return cls(Error, info, None, format_exception(exception))

    @classmethod
    def success(cls, expected, function=None):
        if function is None:
            return cls.trace_current_frame(Success, expected, None)
        else:
            return cls(Success, Context.from_callable(function),
                       expected, None)


class TestAttempt(object):
    def __init__(self, suite, exception=None, function=None):
        self.suite = suite
        self.exception = exception
        self.name = suite.name
        if suite.name is None and function is not None:
            self.name = function.__name__
        self.function = function

    def __enter__(self):
        self.suite.report_start(self.name)
        return self

    def __exit__(self, exc, error, tb):
        if exc is self.exception:
            # Expected exception (or lack thereof) was raised
            result = TestResult.success(exc, self.function)
        elif exc is AssertionError:
            # Failed assertion
            result = TestResult.failed_assertion(error, tb)
        elif self.exception is not None:
            # Failed to raise expected exception
            result = TestResult.failed_to_raise(self.exception, error)
        else:
            # Unexpected error
            result = TestResult.error_from_exception(error)
        result.attempt = self
        self.suite.report(result)
        self.suite.report_finish(self.name)
        return True


class TestSuite(object):
    def __init__(self, name=None):
        self.name = name
        self.results = []
        self.parent = None

    @property
    def failures(self):
        return (result for result in self.results if result.status == Failure)

    @property
    def successes(self):
        return (result for result in self.results if result.status == Success)

    @property
    def errors(self):
        return (result for result in self.results if result.status == Error)

    def catch(self, exception=None):
        return TestAttempt(self, exception=exception)

    def get_child(self, name):
        child = self.__class__()
        child.parent = self
        child.results = self.results
        child.name = '{}:{}'.format(self.name, name) if self.name else name
        return child

    def test(self, function, args=(), kwargs={}, exception=None, name=None):
        suite = self.get_child(name or function.__name__)
        with TestAttempt(suite, exception, function) as attempt:
            result = function(*args, **kwargs)
            if hasattr(result, '__tests__'):
                result.__tests__(self)
        return attempt

    def __iter__(self):
        return iter(self.results)

    def exit(self):
        if any(self.errors):
            exit(2)
        if any(self.failures):
            exit(1)
        exit(0)

    def report_start(self, name):
        pass

    def report_finish(self, name):
        pass

    def report_success(self, result):
        pass

    def report_failure(self, result):
        pass

    def report_error(self, result):
        pass

    def report(self, result):
        self.results.append(result)
        if result.status == ResultStatus.Success:
            self.report_success(result)
        elif result.status == ResultStatus.Failure:
            self.report_failure(result)
        elif result.status == ResultStatus.Error:
            self.report_error(result)
        else:
            raise ValueError("Cannot report {0.status}".format(result))

    def summary(self):
        self.report_summary(dict(
            attempts=len(self.results),
            failures=len(list(self.failures)),
            errors=len(list(self.errors)),
            successes=len(list(self.successes)),
        ))

    def run_module_docstrings(self, module, **options):
        module = import_module(module)
        finder = doctest.DocTestFinder(exclude_empty=False)
        for test in finder.find(module, module.__name__):
            runner = DocstringRunner(self, **options)
            runner.run(test)
            for status, test, example, got in runner.results:
                self.report(TestResult(status, Context(
                    file=test.filename,
                    module=test.name,
                    name='__doc__',
                    line=test.lineno + example.lineno + 1,
                    source_block=example.source.strip(),
                ), expected=example.want.rstrip(), actual=got.rstrip()))

    def run_package_docstrings(self, package, **options):
        for module in find_package_module_names(package):
            self.run_module_docstrings(module, **options)


class DocstringRunner(doctest.DocTestRunner):
    def __init__(self, suite, **options):
        doctest.DocTestRunner.__init__(self, **options)
        self.results = []

    def report_success(self, out, test, example, got):
        self.results.append((Success, test, example, got))

    def report_failure(self, out, test, example, got):
        self.results.append((Failure, test, example, got))

    def report_unexpected_exception(self, out, test, example, got):
        self.results.append((Error, test, example, got))


class LoggingTestSuite(TestSuite):
    def __init__(self, name=None):
        TestSuite.__init__(self, name)
        self.loggers = [logging.getLogger(name)]
        self.result_format = ""
        self.summary_format = ""

    def push_logger(self, name):
        self.loggers.append(self.loggers[-1].getChild(name))

    def pop_logger(self):
        if len(self.loggers) == 1:
            raise IndexError("Cannot pop last logger")
        return self.loggers.pop(-1)

    @property
    def logger(self):
        return self.loggers[-1]

    def report(self, result):
        self.logger.debug(result.source)
        super(LoggingTestSuite, self).report(result)

    def report_start(self, name):
        if name:
            self.push_logger(name)

    def report_success(self, result):
        self.logger.info(self.result_format.format(**vars(result)))

    def report_failure(self, result):
        self.logger.warning(self.result_format.format(**vars(result)))

    def report_error(self, result):
        self.logger.error(self.result_format.format(**vars(result)))

    def report_summary(self, results):
        self.logger.info(self.summary_format.format(**results))

    def report_finish(self, name):
        if name:
            self.pop_logger()
