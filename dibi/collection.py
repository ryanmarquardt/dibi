#!/usr/bin/env python

from collections import OrderedDict, MutableSet, MutableMapping, Mapping, Set
from abc import ABCMeta, abstractmethod


class KeyValue(object):
    """

    >>> KeyValue('a', 1).get_key()
    'a'

    >>> KeyValue('a', 1) == KeyValue('a', 3)
    True

    """

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def get_key(self):
        return self.key

    def __repr__(self):
        return "KeyValue({key!r}, {value!r})".format(**self.__dict__)

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if isinstance(other, KeyValue):
            return self.key == other.key
        return NotImplemented


class Collection(MutableMapping, MutableSet):
    """Set with item access by key

    >>> collection = Collection(KeyValue.get_key)

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

    Collections exhibit features of both sets and dicts, and can be converted
    to either type.

    >>> set(collection)
    {KeyValue('a', 1)}

    >>> dict(collection)
    {'a': KeyValue('a', 1)}

    """

    def __init__(self, key_function, *items, **kwargs):
        self.__key_function = key_function
        self.__items = kwargs.pop('storage', dict())
        for item in items:
            self.add(item)

    def __repr__(self):
        """

        >>> Collection(KeyValue.get_key)
        Collection()

        >>> Collection(KeyValue.get_key, KeyValue('a', 1))
        Collection(KeyValue('a', 1))

        """
        return "Collection({})".format(", ".join(repr(item) for item in self))

    def __delitem__(self, key):
        """Remove an item by key

        >>> c = Collection(KeyValue.get_key, KeyValue('a', 1))

        >>> del c['a']

        >>> len(c)
        0

        """
        del self.__items[key]

    def __getitem__(self, key):
        """Retrieve an item by key

        >>> c = Collection(KeyValue.get_key, KeyValue('a', 1))

        >>> c['a']
        KeyValue('a', 1)

        """
        return self.__items[key]

    def keys(self):
        """Return keys of all members

        >>> list(Collection(KeyValue.get_key, KeyValue('a', 1)).keys())
        ['a']

        """
        return self.__items.keys()

    def values(self):
        """Returns an iterator over collection members. Alias for
        iter(Collection).

        >>> list(Collection(KeyValue.get_key, KeyValue('a', 1)).values())
        [KeyValue('a', 1)]

        """
        return iter(self)

    def items(self):
        """Returns an iterator over collection keys and corresponding members.

        >>> list(Collection(KeyValue.get_key, KeyValue('a', 1)).items())
        [('a', KeyValue('a', 1))]
        """
        return self.__items.items()

    def __iter__(self):
        """Iterate over collection members

        >>> c = OrderedCollection(KeyValue.get_key, KeyValue('a', 1))

        >>> _ = c.add(KeyValue('b', 2))

        >>> for item in c:
        ...   print(item)
        KeyValue('a', 1)
        KeyValue('b', 2)

        """
        return (value for value in self.__items.values())

    def __len__(self):
        """Returns the number of items in the collection

        >>> len(Collection(KeyValue.get_key))
        0

        >>> c = Collection(KeyValue.get_key)

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

        >>> Collection(KeyValue.get_key)['a'] = None
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

        >>> c = Collection(KeyValue.get_key, KeyValue('a', 1))

        >>> c.add(KeyValue('a', 2))
        KeyValue('a', 2)

        >>> c.add(KeyValue('a', 3), replace=False)
        KeyValue('a', 2)

        """
        key = self.__key_function(item)
        if replace or key not in self.__items:
            self.__items[key] = item
            return item
        else:
            return self.__items[key]

    def discard(self, item):
        """Remove an item if it is present.

        Equivalent to Collection.__delitem__(key(item))

        >>> a = KeyValue('a', 1)

        >>> c = Collection(KeyValue.get_key, a)

        >>> c.discard(a)

        >>> len(c)
        0

        >>> c.discard(KeyValue('b', 2))

        """
        self.__items.pop(self.__key_function(item), None)

    def remove(self, item):
        """Remove an item, raising KeyError if it is not present.

        >>> c = Collection(KeyValue.get_key, KeyValue('a', 1))

        >>> c.remove(KeyValue('a', 1))

        >>> len(c)
        0

        >>> c.remove(KeyValue('c', 3))
        Traceback (most recent call last):
         ...
        KeyError: 'c'
        """
        del self.__items[self.__key_function(item)]

    def __contains__(self, key_or_item):
        """Check for containment by key or identity.

        >>> a = KeyValue('a', 1)

        >>> c = Collection(KeyValue.get_key, a)

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
            key = self.__key_function(key_or_item)
        except AttributeError:
            return False
        return key in self.__items

    def __eq__(self, other):
        """

        >>> c = Collection(KeyValue.get_key, KeyValue('a', 1))

        >>> c == Collection(KeyValue.get_key, KeyValue('a', 1))
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

    >>> c = OrderedCollection(KeyValue.get_key, KeyValue('a', 1))

    >>> c.add(KeyValue('c', 3))
    KeyValue('c', 3)

    >>> c.add(KeyValue('b', 2))
    KeyValue('b', 2)

    >>> for item in c:
    ...   print(item.key, item.value)
    a 1
    c 3
    b 2

    """
    def __init__(self, key_function, *items):
        Collection.__init__(self, key_function, storage=OrderedDict(), *items)
