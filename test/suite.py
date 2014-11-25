

from contextlib import contextmanager
import doctest
from importlib import import_module
import inspect
import logging
import os


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


class TestResult(object):
    def __init__(self, location, lineno, source, locals, exception=None):
        self.location = location
        self.lineno = lineno
        self.source = source
        self.locals = locals
        self.exception = exception

    status = 'unknown'

    @classmethod
    def from_traceback(cls, traceback, exception=None):
        tb = traceback
        while tb.tb_next:
            tb = tb.tb_next
        frame = tb.tb_frame
        code = frame.f_code
        source, first_line = inspect.getsourcelines(tb)
        source = source[frame.f_lineno - first_line]
        location = inspect.getmoduleinfo(code.co_filename).name
        if code.co_name:
            location = '.'.join((location, code.co_name))
        return cls(
            location=location,
            lineno=frame.f_lineno,
            source=source.strip(),
            locals={key: value for key, value in frame.f_locals.items()
                    if key in source},
            exception=exception,
        )

    def __str__(self):
        return '{status} at line {lineno}'.format(
            status=self.status.title(),
            lineno=self.lineno,
            source=self.source,
        )


class FailureResult(TestResult):

    status = 'failure'

    @classmethod
    def from_assertion(cls, assertion):
        return cls.from_traceback(assertion.__traceback__)

    @classmethod
    def did_not_raise(cls, exception):
        return cls(
            location=None,
            lineno='???',
            source='???',
            locals={},
            exception=exception
        )

    def __str__(self):
        return '{} '.format(
            TestResult.__str__(self),
        )


class SuccessResult(TestResult):
    status = 'success'


class ErrorResult(TestResult):
    status = 'error'

    @classmethod
    def from_exception(cls, exception):
        return cls.from_traceback(exception.__traceback__, exception)


class DocstringRunner(doctest.DocTestRunner):
    def __init__(self, suite):
        doctest.DocTestRunner.__init__(self)
        self.suite = suite

    def report_success(self, out, test, example, got):
        # out is sys.stderr or other writable ... ignore it
        # test
        #   .name is the id of the test
        # example is the current member of test.examples being run
        #
        # got is the printed output ... ignore it on success
        ## SuccessResult(module, function, lineno, source, locals)
        self.suite.report(SuccessResult(
            test.name, test.lineno + example.lineno,
            example.source.strip(), test.globs))

    def report_failure(self, out, test, example, got):
        raise Exception(('failure', out, test, example, got))

    def report_unexpected_exception(self, out, test, example, got):
        raise Exception(('error', out, test, example, got))


class TestAttempt(object):
    def __init__(self, suite, exception=None, name=None):
        self.suite = suite
        self.exception = exception
        self.name = name

    def __enter__(self):
        if self.name:
            self.suite.push_logger(self.name)
        self.suite.report_start(self)

    def __exit__(self, exc, error, tb):
        try:
            if exc is AssertionError:
                # Failed assertion
                result = FailureResult.from_traceback(tb)
            elif self.exception is None:
                # Expected success
                result = (SuccessResult(None, None, None, None) if exc is None else
                          ErrorResult.from_exception(error))
            else:
                # Expected exception
                result = (SuccessResult(None, None, None, None) if exc is self.exception else
                          FailureResult.did_not_raise(self.exception))
        except Exception as metaerror:
            logging.critical(str(metaerror))
        self.suite.report(result)
        if self.name:
            self.suite.pop_logger()
        return True


class TestSuite(object):
    def __init__(self, name=None):
        self.loggers = [logging.getLogger(name)]
        self.results = []

    def push_logger(self, name):
        self.loggers.append(self.loggers[-1].getChild(name))

    def pop_logger(self):
        if len(self.loggers) == 1:
            raise IndexError("Cannot pop last logger")
        return self.loggers.pop(-1)

    @property
    def logger(self):
        return self.loggers[-1]

    @property
    def failures(self):
        return (result for result in self.results
                if isinstance(result, FailureResult))

    @property
    def successes(self):
        return [result for result in self.results
                if isinstance(result, SuccessResult)]

    @property
    def errors(self):
        return [result for result in self.results
                if isinstance(result, ErrorResult)]

    def catch(self, exception=None, name=None):
        return TestAttempt(self, exception=exception, name=name)

    def __iter__(self):
        return iter(self.results)

    def exit(self):
        if any(self.errors):
            exit(2)
        if any(self.failures):
            exit(1)
        exit(0)

    def report(self, result):
        if isinstance(result, SuccessResult):
            self.report_success(result)
        elif isinstance(result, FailureResult):
            self.report_failure(result)
        elif isinstance(result, ErrorResult):
            self.report_error(result)
        else:
            raise ValueError("Cannot report {!r}".format(result))
        self.logger.debug(result.source)
        self.results.append(result)

    def report_start(self, attempt):
        logging.debug("Expecting {}".format(
            'success' if attempt.exception is None else
            attempt.exception.__name__,
        ))

    def report_success(self, result):
        self.logger.info(str(result))

    def report_failure(self, result):
        self.logger.warning('{}\n{}\n{}'.format(
            result,
            result.source,
            result.locals,
        ))

    def report_error(self, result):
        self.logger.error(str(result))
        self.logger.error(str(result.exception))

    def run_module_docstrings(self, module):
        try:
            module = import_module(module)
            finder = doctest.DocTestFinder(exclude_empty=False)
            runner = DocstringRunner(self)
            for test in finder.find(module, module.__name__):
                logger = self.logger.getChild(test.name).getChild('__doc__')
                self.push_logger('{}.__doc__'.format(test.name))
                try:
                    runner.run(test)
                finally:
                    self.pop_logger()
        except Exception as error:
            self.report(ErrorResult.from_exception(error))
            return

    def run_package_docstrings(self, package):
        for module in find_package_module_names(package):
            self.run_module_docstrings(module)
