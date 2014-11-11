#!/usr/bin/env python

from collections import OrderedDict, MutableSet, MutableMapping, Mapping, Set
import abc


class Collectable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __key__(self):
        return ''

    def __hash__(self):
        return hash(self.__key__())

    def __eq__(self, other):
        if isinstance(other, Collectable):
            return self.__key__() == other.__key__()


class KeyValue(Collectable):
    """

    >>> KeyValue('a', 1).__key__()
    'a'

    >>> KeyValue('a', 1) == KeyValue('a', 3)
    True

    """

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __key__(self):
        return self.key

    def __repr__(self):
        return "KeyValue({key!r}, {value!r})".format(**self.__dict__)


class Collection(MutableMapping, MutableSet):
    """Set with item access by key

    >>> collection = Collection()

    >>> a = collection.add(KeyValue('a', 1))

    >>> a in collection
    True

    >>> 'a' in collection
    True

    >>> collection['a']
    KeyValue('a', 1)


    >>> list(collection.items())
    [('a', KeyValue('a', 1))]

    >>> collection.get('a')
    KeyValue('a', 1)

    >>> collection.get('b')

    """

    def __init__(self, *items, **kwargs):
        self.__items = kwargs.pop('storage', dict())
        for item in items:
            self.add(item)

    def __repr__(self):
        """

        >>> Collection()
        Collection()

        >>> Collection(KeyValue('a', 1))
        Collection(KeyValue('a', 1))

        """
        return "Collection({})".format(", ".join(repr(item) for item in self))

    def __delitem__(self, key):
        """Remove an item by key

        >>> c = Collection(KeyValue('a', 1))

        >>> del c['a']

        >>> len(c)
        0

        """
        del self.__items[key]

    def __getitem__(self, key):
        """Retrieve an item by key

        >>> c = Collection(KeyValue('a', 1))

        >>> c['a']
        KeyValue('a', 1)

        """
        return self.__items[key]

    def keys(self):
        """Return keys of all members

        >>> list(Collection(KeyValue('a', 1)).keys())
        ['a']

        """
        return self.__items.keys()

    def values(self):
        """Returns an iterator over collection members. Alias for
        iter(Collection).

        >>> list(Collection(KeyValue('a', 1)).values())
        [KeyValue('a', 1)]

        """
        return iter(self)

    def items(self):
        """Returns an iterator over collection keys and corresponding members.

        >>> list(Collection(KeyValue('a', 1)).items())
        [('a', KeyValue('a', 1))]
        """
        return self.__items.items()

    def __iter__(self):
        """Iterate over collection members

        >>> c = OrderedCollection(KeyValue('a', 1), KeyValue('b', 2))

        >>> for item in c:
        ...   print(item)
        KeyValue('a', 1)
        KeyValue('b', 2)

        """
        return (value for value in self.__items.values())

    def __len__(self):
        """Returns the number of items in the collection

        >>> len(Collection())
        0

        >>> c = Collection()

        >>> c.add(KeyValue('a', 1))
        KeyValue('a', 1)

        >>> c.add(KeyValue('b', 2))
        KeyValue('b', 2)

        >>> len(c)
        2

        """
        return len(self.__items)

    def __setitem__(self, key, value):
        """Setting items is not supported

        >>> Collection()['a'] = None
        Traceback (most recent call last):
         ...
        NotImplementedError: __setitem__ is not supported. Use Collection.add()

        """
        raise NotImplementedError("__setitem__ is not supported. "
                                  "Use Collection.add()")

    def add(self, item, replace=True):
        """Add an item to the collection.

        The item provided is added only if replace is true (the default) or if
        the collection doesn't already contain an item with the same key.

        Returns the item which exists in the collection after addition.

        >>> c = Collection(KeyValue('a', 1))

        >>> c.add(KeyValue('a', 2))
        KeyValue('a', 2)

        >>> c.add(KeyValue('a', 3), replace=False)
        KeyValue('a', 2)

        """
        key = item.__key__()
        if replace or key not in self.__items:
            self.__items[key] = item
            return item
        else:
            return self.__items[key]

    def discard(self, item):
        """Remove an item if it is present.

        Equivalent to Collection.__delitem__(item.__key__())

        >>> a = KeyValue('a', 1)

        >>> c = Collection(a)

        >>> c.discard(a)

        >>> len(c)
        0

        >>> c.discard(KeyValue('b', 2))

        """
        self.__items.pop(item.__key__(), None)

    def remove(self, item):
        """Remove an item, raising KeyError if it is not present.

        >>> c = Collection(KeyValue('a', 1))

        >>> c.remove(KeyValue('a', 1))

        >>> len(c)
        0

        >>> c.remove(KeyValue('c', 3))
        Traceback (most recent call last):
         ...
        KeyError: 'c'
        """
        del self.__items[item.__key__()]

    def __contains__(self, key_or_item):
        """Check for containment by key or identity.

        >>> a = KeyValue('a', 1)

        >>> c = Collection(a)

        >>> a in c
        True

        >>> 'a' in c
        True

        >>> 'b' in c
        False

        """
        try:
            has_item = key_or_item in self.__items
        except TypeError:
            has_item = False
        if has_item:
            return True
        try:
            key = key_or_item.__key__()
        except AttributeError:
            return False
        return key in self.__items

    def __eq__(self, other):
        """

        >>> c = Collection(KeyValue('a', 1))

        >>> c == Collection(KeyValue('a', 1))
        True

        >>> c == {KeyValue('a', 1)}
        True

        """
        if isinstance(other, Collection):
            return other.__items == self.__items
        elif isinstance(other, Mapping):
            return other == self.__items
        elif isinstance(other, Set):
            return set(self.__items.values()) == other
        else:
            return NotImplemented


class OrderedCollection(Collection):
    """Collection which preserves order in which items are added

    >>> c = OrderedCollection(KeyValue('a', 1), KeyValue('c', 3))

    >>> c.add(KeyValue('b', 2))
    KeyValue('b', 2)

    >>> for item in c:
    ...   print(item.key, item.value)
    a 1
    c 3
    b 2

    """
    def __init__(self, *items):
        Collection.__init__(self, storage=OrderedDict(), *items)
