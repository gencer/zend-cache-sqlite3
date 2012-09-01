Zend_Cache SQLite3 support for Zend Framework 1.11.xx
======

Blog: [http://gencergenc.wordpress.com/2012/01/28/zend-zend_cache-with-sqlite3-support]

###Installation

First of all, choose a way to implement your cache.

1. Native built-in SQLite3 feature (in native folder)
2. PDO

Download the SQLite3.php file and copy it under the ./library/Zend/Cache/Backend/ folder.

Change your "Sqlite" parameter on cache factory to **Sqlite3**.

Thats all.

###Credits

SQLite3 support is Implemented by **Gencer Genç**

General Improvements and Fixes by **John Crenshaw**.

Performance Improvements by **@wilddom** [https://github.com/wilddom]

Fix for remove() by **@sitnikov** [https://github.com/sitnikov]