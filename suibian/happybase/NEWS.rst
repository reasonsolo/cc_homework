Version history
===============

.. py:currentmodule:: happybase

HappyBase 0.5 (not yet released)
--------------------------------

Note: this release is a work in progress!

* The :py:meth:`Connection.delete_table` method now features an optional
  `disable` parameter to make deleting enabled tables easier.

* The debug log message emitted by :py:meth:`Table.scan` when closing a scanner
  now includes both the number of rows returned to the calling code, and also
  the number of rows actually fetched from the server. If scanners are not
  completely iterated over (e.g. because of a 'break' statement in the for loop
  for the scanner), these numbers may differ. If this happens often, and the
  differences are big, this may be a hint that the `batch_size` parameter to
  :py:meth:`Table.scan()` is not optimal for your application.

HappyBase 0.4
-------------

Release date: 2012-07-11

* Add an optional `table_prefix_separator` argument to the
  :py:class:`Connection` constructor, to specify the prefix used for the
  `table_prefix` argument (`issue #3
  <https://github.com/wbolster/happybase/issues/3>`_)
* Add support for framed Thrift transports using a new optional `transport`
  argument to :py:class:`Connection` (`issue #6
  <https://github.com/wbolster/happybase/issues/6>`_)
* Add the Apache license conditions in the :doc:`license statement <license>`
  (for the included HBase parts)
* Documentation improvements


HappyBase 0.3
-------------

Release date: 2012-05-25

New features:

* Improved compatibility with HBase 0.90.x

  * In earlier versions, using :py:meth:`Table.scan` in combination with HBase
    0.90.x often resulted in crashes, caused by incompatibilities in the
    underlying Thrift protocol.
  * A new `compat` flag to the :py:class:`Connection` constructor has been
    added to enable compatibility with HBase 0.90.x.
  * Note that the :py:meth:`Table.scan` API has a few limitations when used
    with HBase 0.90.x.

* The `row_prefix` argument to :py:meth:`Table.scan` can now be used together
  with `filter` and `timestamp` arguments.

Other changes:

* Lower Thrift dependency to 0.6
* The `setup.py` script no longer installs the tests
* Documentation improvements


HappyBase 0.2
-------------

Release date: 2012-05-22

* Fix package installation, so that ``pip install happybase`` works as expected
  (`issue #1 <https://github.com/wbolster/happybase/issues/1>`_)
* Various small documentation improvements


HappyBase 0.1
-------------

Release date: 2012-05-20

* Initial release
