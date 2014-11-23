
import traceback
from contextlib import contextmanager


class Failure(Exception):
    pass


class Result(object):
    def __init__(self, tb, title, message, *args):
        self.tb = tb
        self.title = title
        self.message = message
        self.args = args

    def __str__(self):
        return "Failure in {title!r}:\n  {message}".format(
            title=self.title,
            message=self.message.format(*self.args),
        )


class Test(object):
    def __init__(self, *args, **kwargs):
        self.failed = False

    def __call__(self, comment):
        self.comment = comment
        return self

    def __enter__(self):
        return self

    def report_failure(self, tb, comment, condition, *args):
        self.failed = True
        logging.error("{message} in {comment!r}".format(
            tb=tb,
            comment=comment,
            message=condition.format(*args),
        ))

    def report_error(self, tb, comment, exc, obj):
        self.failed = True
        logging.critical(("Unhandled exception in {comment!r}:\n"
                          "    {error}").format(
            tb=tb,
            comment=comment,
            error=''.join(traceback.format_exception_only(exc, obj)),
        ))

    def __exit__(self, exc, obj, tb):
        comment = self.comment
        del self.comment
        if exc is Failure:
            self.report_failure(tb, comment, *obj.args)
            return True
        if exc is not None:
            self.report_error(tb, comment, exc, obj)
            return True

    def instance(self, value, baseclass):
        if not isinstance(value, baseclass):
            raise Failure('{0} is instance of {1}', value, baseclass)

    def equal(self, first, second):
        if first != second:
            raise Failure('{0} != {1}', first, second)

    def identical(self, first, second):
        if first is not second:
            raise Failure('{0} is not {1}', first, second)

    def contains(self, container, item):
        if item not in container:
            raise Failure('{1!r} not in {0!r}', container, item)

    @contextmanager
    def raises(self, exception):
        try:
            yield
        except exception as error:
            return
        raise Failure('{0} not raised', exception)
