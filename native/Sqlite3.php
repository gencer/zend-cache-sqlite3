<?php
/**
 * Zend Framework
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.txt.
 * It is also available through the world-wide-web at this URL:
 * http://framework.zend.com/license/new-bsd
 * If you did not receive a copy of the license and are unable to
 * obtain it through the world-wide-web, please send an email
 * to license@zend.com so we can send you a copy immediately.
 * 
 * SQLite3 support is Implemented by Gencer Genç, Improved and Fixed By John Crenshaw.
 * 
 * To follow this modification changes, 
 * visit http://gencergenc.wordpress.com/2012/01/28/zend-zend_cache-with-sqlite3-support/
 *
 * NOTE: This mod only for PHP's native built-in Sqlite3 support.
 *       For PDO, visit my blog.
 *       
 * @category   Zend
 * @package    Zend_Cache
 * @subpackage Zend_Cache_Backend
 * @copyright  Copyright (c) 2005-2011 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 * @version    $Id: Sqlite3.php 24400 2012-04-12 22:19:00Z john $
 */


/**
 * @see Zend_Cache_Backend_Interface
 */
require_once 'Zend/Cache/Backend/ExtendedInterface.php';

/**
 * @see Zend_Cache_Backend
 */
require_once 'Zend/Cache/Backend.php';

/**
 * @package    Zend_Cache
 * @subpackage Zend_Cache_Backend
 * @copyright  Copyright (c) 2005-2011 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 */
class Zend_Cache_Backend_Sqlite3 extends Zend_Cache_Backend implements Zend_Cache_Backend_ExtendedInterface
{
    /**
     * Available options
     *
     * =====> (string) cache_db_complete_path :
     * - the complete path (filename included) of the SQLITE database
     *
     * ====> (int) automatic_vacuum_factor :
     * - Disable / Tune the automatic vacuum process
     * - The automatic vacuum process defragment the database file (and make it smaller)
     *   when a clean() or delete() is called
     *     0               => no automatic vacuum
     *     1               => systematic vacuum (when delete() or clean() methods are called)
     *     x (integer) > 1 => automatic vacuum randomly 1 times on x clean() or delete()
     *
     * @var array Available options
     */
    protected $_options = array(
        'cache_db_complete_path' => null,
        'automatic_vacuum_factor' => 10
    );

    /**
     * DB ressource
     *
     * @var mixed $_db
     */
    private $_db = null;

    /**
     * Boolean to store if the structure has benn checked or not
     *
     * @var boolean $_structureChecked
     */
    private $_structureChecked = false;

    /**
     * Boolean set when repairing after an error
     *
     * @var boolean $_repairing
     */
    private $_repairing = false;
    
    /**
     * Constructor
     *
     * @param  array $options Associative array of options
     * @throws Zend_cache_Exception
     * @return void
     */
    public function __construct(array $options = array())
    {
        parent::__construct($options);
        if ($this->_options['cache_db_complete_path'] === null) {
            Zend_Cache::throwException('cache_db_complete_path option has to set');
        }
        if (!extension_loaded('sqlite3')) {
            Zend_Cache::throwException("Cannot use SQLite3 storage because the 'sqlite3' extension is not loaded in the current PHP environment");
        }
        $this->_getConnection();
    }

    /**
     * Destructor
     *
     * @return void
     */
    public function __destruct()
    {
       $this->_db->close();
    }

    /**
     * Test if a cache is available for the given id and (if yes) return it (false else)
     *
     * @param  string  $id                     Cache id
     * @param  boolean $doNotTestCacheValidity If set to true, the cache validity won't be tested
     * @return string|false Cached datas
     */
    public function load($id, $doNotTestCacheValidity = false)
    {
        $this->_checkAndBuildStructure();
        $sql = "SELECT content FROM cache WHERE id=?";
        $params = array($id);
        if (!$doNotTestCacheValidity) {
            $sql = $sql . " AND (expire=0 OR expire>?)";
            $params[] = time();
        }
        $result = $this->_query($sql, $params);
        $row = @$result->fetchArray();
        if ($row) {
            return $row['content'];
        }
        return false;
    }

    /**
     * Test if a cache is available or not (for the given id)
     *
     * @param string $id Cache id
     * @return mixed|false (a cache is not available) or "last modified" timestamp (int) of the available cache record
     */
    public function test($id)
    {
        $this->_checkAndBuildStructure();
        $sql = "SELECT lastModified FROM cache WHERE id=? AND (expire=0 OR expire>?)";
        $result = $this->_query($sql, array($id, time()));
        $row = @$result->fetchArray();
        if ($row) {
            return ((int) $row['lastModified']);
        }
        return false;
    }

    /**
     * Save some string datas into a cache record
     *
     * Note : $data is always "string" (serialization is done by the
     * core not by the backend)
     *
     * @param  string $data             Datas to cache
     * @param  string $id               Cache id
     * @param  array  $tags             Array of strings, the cache record will be tagged by each string entry
     * @param  int    $specificLifetime If != false, set a specific lifetime for this cache record (null => infinite lifetime)
     * @throws Zend_Cache_Exception
     * @return boolean True if no problem
     */
    public function save($data, $id, $tags = array(), $specificLifetime = false)
    {
        $this->_checkAndBuildStructure();
        $lifetime = $this->getLifetime($specificLifetime);
        $mktime = time();
        if ($lifetime === null) {
            $expire = 0;
        } else {
            $expire = $mktime + $lifetime;
        }
        $this->_query("BEGIN TRANSACTION");
        $this->_query("DELETE FROM cache WHERE id=?", array($id));
        $sql = "INSERT INTO cache (id, content, lastModified, expire) VALUES (?, ?, ?, ?)";
        $res = $this->_query($sql, array($id, $data, $mktime, $expire));
        if (!$res) {
            $this->_query("ROLLBACK TRANSACTION");
            $this->_log("Zend_Cache_Backend_Sqlite::save() : impossible to store the cache id=$id");
            return false;
        }
        $res = true;
        foreach ($tags as $tag) {
            $res = $this->_registerTag($id, $tag) && $res;
        }
        $this->_query("COMMIT TRANSACTION");
        return $res;
    }

    /**
     * Remove a cache record
     *
     * @param  string $id Cache id
     * @return boolean True if no problem
     */
    public function remove($id)
    {
        $this->_checkAndBuildStructure();
        $result1 = @$this->_db->querySingle("SELECT COUNT(*) AS nbr FROM cache WHERE id='$id'");
        $result2 = $this->_query("DELETE FROM cache WHERE id=?", array($id));
        $result3 = $this->_query("DELETE FROM tag WHERE id=?", array($id));
        $this->_automaticVacuum();
        return ($result1 && $result2 && $result3);
    }

    /**
     * Clean some cache records
     *
     * Available modes are :
     * Zend_Cache::CLEANING_MODE_ALL (default)    => remove all cache entries ($tags is not used)
     * Zend_Cache::CLEANING_MODE_OLD              => remove too old cache entries ($tags is not used)
     * Zend_Cache::CLEANING_MODE_MATCHING_TAG     => remove cache entries matching all given tags
     *                                               ($tags can be an array of strings or a single string)
     * Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG => remove cache entries not {matching one of the given tags}
     *                                               ($tags can be an array of strings or a single string)
     * Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG => remove cache entries matching any given tags
     *                                               ($tags can be an array of strings or a single string)
     *
     * @param  string $mode Clean mode
     * @param  array  $tags Array of tags
     * @return boolean True if no problem
     */
    public function clean($mode = Zend_Cache::CLEANING_MODE_ALL, $tags = array())
    {
        $this->_checkAndBuildStructure();
        $return = $this->_clean($mode, $tags);
        $this->_automaticVacuum();
        return $return;
    }

    /**
     * Return an array of stored cache ids
     *
     * @return array array of stored cache ids (string)
     */
    public function getIds()
    {
        $this->_checkAndBuildStructure();
        $result = array();
        while ($id = @$this->_db->querySingle("SELECT id FROM cache WHERE (expire=0 OR expire>" . time() . ")")) {
            $result[] = $id;
        }
        return $result;
    }

    /**
     * Return an array of stored tags
     *
     * @return array array of stored tags (string)
     */
    public function getTags()
    {
        $this->_checkAndBuildStructure();
       // $res = $this->_query("SELECT DISTINCT(name) AS name FROM tag");
        $result = array();
        while ($id = @$this->_db->querySingle("SELECT DISTINCT(name) AS name FROM tag")) {
            $result[] = $id;
        }
        return $result;
    }

    /**
     * Return an array of stored cache ids which match given tags
     *
     * In case of multiple tags, a logical AND is made between tags
     *
     * @param array $tags array of tags
     * @return array array of matching cache ids (string)
     */
    public function getIdsMatchingTags($tags = array())
    {
        $first = true;
        $ids = array();
        foreach ($tags as $tag) {
            $res = $this->_query("SELECT DISTINCT(id) AS id FROM tag WHERE name=?", array($tag));
            
            $rows = @$res->fetchArray(SQLITE3_ASSOC);
 
            if(!$rows) return array();
          // var_dump($rows);
            $ids2 = array();
            
            foreach ($rows as $key => $row) {
            	$row = (object)$row;
            	$ids2[] = $row->id;
            }
            
            if ($first) {
                $ids = $ids2;
                $first = false;
            } else {
                $ids = array_intersect($ids, $ids2);
            }
        }
        $result = array();
        foreach ($ids as $id) {
            $result[] = $id;
        }
        return $result;
    }

    /**
     * Return an array of stored cache ids which don't match given tags
     *
     * In case of multiple tags, a logical OR is made between tags
     *
     * @param array $tags array of tags
     * @return array array of not matching cache ids (string)
     */
    public function getIdsNotMatchingTags($tags = array())
    {
        $res = $this->_query("SELECT id FROM cache");
        $rows = @$res->fetchArray(SQLITE3_ASSOC);
        
        $result = array();
        foreach ($rows as $row) {
            $id = $row['id'];
            $matching = false;
            foreach ($tags as $tag) {

            	$res = @$this->_db->querySingle("SELECT COUNT(*) AS nbr FROM tag WHERE name='$tag' AND id='$id'");
                $nbr = (int) $res;
                if ($nbr > 0) {
                    $matching = true;
                }
                
            }
            if (!$matching) {
                $result[] = $id;
            }
        }
        return $result;
    }

    /**
     * Return an array of stored cache ids which match any given tags
     *
     * In case of multiple tags, a logical AND is made between tags
     *
     * @param array $tags array of tags
     * @return array array of any matching cache ids (string)
     */
    public function getIdsMatchingAnyTags($tags = array())
    {
        $first = true;
        $ids = array();
        foreach ($tags as $tag) {
            $res = $this->_query("SELECT DISTINCT(id) AS id FROM tag WHERE name=?", array($tag));
            if (!$res) {
                return array();
            }
            $rows = @$res->fetchArray(SQLITE3_ASSOC);
            if(!$rows) return array();
            
            $ids2 = array();
            foreach ($rows as $row) {
                $ids2[] = $row['id'];
            }
            if ($first) {
                $ids = $ids2;
                $first = false;
            } else {
                $ids = array_merge($ids, $ids2);
            }
        }
        $result = array();
        foreach ($ids as $id) {
            $result[] = $id;
        }
        return $result;
    }

    /**
     * Return the filling percentage of the backend storage
     *
     * @throws Zend_Cache_Exception
     * @return int integer between 0 and 100
     */
    public function getFillingPercentage()
    {
        $dir = dirname($this->_options['cache_db_complete_path']);
        $free = disk_free_space($dir);
        $total = disk_total_space($dir);
        if ($total == 0) {
            Zend_Cache::throwException('can\'t get disk_total_space');
        } else {
            if ($free >= $total) {
                return 100;
            }
            return ((int) (100. * ($total - $free) / $total));
        }
    }

    /**
     * Return an array of metadatas for the given cache id
     *
     * The array must include these keys :
     * - expire : the expire timestamp
     * - tags : a string array of tags
     * - mtime : timestamp of last modification time
     *
     * @param string $id cache id
     * @return array array of metadatas (false if the cache id is not found)
     */
    public function getMetadatas($id)
    {
        $tags = array();
        $res = $this->_query("SELECT name FROM tag WHERE id=?", array($id));
       
        $rows = $res->fetchArray(SQLITE3_ASSOC);
        if($rows) {
        	foreach ($rows as $row) {
        		$tags[] = $row['name'];
        	}
        }
        
        $this->_query('CREATE TABLE cache (id TEXT PRIMARY KEY, content BLOB, lastModified INTEGER, expire INTEGER)');
        $res = $this->_query("SELECT lastModified,expire FROM cache WHERE id=?", array($id));
        
        $row = @$res->fetchArray(SQLITE3_ASSOC);
        if (!$row) return false;
        
        return array(
            'tags' => $tags,
            'mtime' => $row['lastModified'],
            'expire' => $row['expire']
        );
    }

    /**
     * Give (if possible) an extra lifetime to the given cache id
     *
     * @param string $id cache id
     * @param int $extraLifetime
     * @return boolean true if ok
     */
    public function touch($id, $extraLifetime)
    {
        
        $expire = @$this->_db->querySingle("SELECT expire FROM cache WHERE id='$id' AND (expire=0 OR expire>" . time() . ')');
        $newExpire = $expire + $extraLifetime;
        $res = $this->_query("UPDATE cache SET lastModified=?, expire=? WHERE id=?", array(time(), $newExpire, $id));
        if ($res) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Return an associative array of capabilities (booleans) of the backend
     *
     * The array must include these keys :
     * - automatic_cleaning (is automating cleaning necessary)
     * - tags (are tags supported)
     * - expired_read (is it possible to read expired cache records
     *                 (for doNotTestCacheValidity option for example))
     * - priority does the backend deal with priority when saving
     * - infinite_lifetime (is infinite lifetime can work with this backend)
     * - get_list (is it possible to get the list of cache ids and the complete list of tags)
     *
     * @return array associative of with capabilities
     */
    public function getCapabilities()
    {
        return array(
            'automatic_cleaning' => true,
            'tags' => true,
            'expired_read' => true,
            'priority' => false,
            'infinite_lifetime' => true,
            'get_list' => true
        );
    }

    /**
     * PUBLIC METHOD FOR UNIT TESTING ONLY !
     *
     * Force a cache record to expire
     *
     * @param string $id Cache id
     */
    public function ___expire($id)
    {
        $time = time() - 1;
        $this->_query("UPDATE cache SET lastModified=?, expire=? WHERE id=?", array($time, $time, $id));
    }

    /**
     * Return the connection resource
     *
     * If we are not connected, the connection is made
     *
     * @throws Zend_Cache_Exception
     * @return resource Connection resource
     */
    private function _getConnection()
    {
        $can_retry = true;
        do
        {
            if ($this->_db) {
                return $this->_db;
            } else {
                $this->_db = @new SQLite3($this->_options['cache_db_complete_path']);
                if (!($this->_db)) {
                    
                    // Automatically recover from unopenable files
                    if (is_file($this->_options['cache_db_complete_path']))
                    {
                        unlink($this->_options['cache_db_complete_path']);
                        if ($can_retry)
                        {
                            $can_retry = false;
                            continue;
                        }
                    }
    
                    Zend_Cache::throwException("Impossible to open " . $this->_options['cache_db_complete_path'] . " cache DB file");
                }
                $this->_db->query("PRAGMA journal_mode=WAL");
                $this->_db->query("PRAGMA synchronous=NORMAL");
                return $this->_db;
            }
        } while (true);
    }

    /**
     * Execute an SQL query silently
     *
     * @param string $query SQL query
     * @return mixed|false query results
     */
    private function _query($query, array $params = array())
    {
        $can_retry = true;
        do
        {
            $db = $this->_getConnection();
            if ($db) {
                if (count($params))
                {
                    $query = @$db->prepare($query);
                    if ($query)
                    {
                        $idx = 0;
                        foreach ($params as &$v)
                        {
                            $idx++;
                            if (is_string($v) && strpos($v, chr(0)))
                            {
                                // binary. Bind as a blob.
                                @$query->bindParam($idx, $v, SQLITE3_BLOB);
                            }
                            else
                            {
                                // The type can be correctly detected automatically
                                @$query->bindParam($idx, $v);
                            }
                        }
                        $res = @$query->execute();
                    }
                    else
                    {
                        $res = false;
                    }
                }
                else
                {
                    $res = @$db->query($query);
                }
                if (!$res) {
                    if (!$this->_repairing)
                    {
                        $this->_repairCorruption();
    
                        if ($can_retry)
                        {
                            $can_retry = false;
                            continue;
                        }
                    }

                    return false;
                } else {
                    return $res;
                }
            }
            return false;
        } while (true);
    }

    private function _repairCorruption()
    {
        $this->_repairing = true;
        $db = $this->_getConnection();
        if (!$this->_haveTable('version', array('num')) ||
            !$this->_haveTable('cache', array('id', 'content', 'lastModified', 'expire')) ||
            !$this->_haveTable('tag', array('name', 'id')) ||
            !$this->_haveIndex('tag_id_index') ||
            !$this->_haveIndex('tag_name_index') ||
            !$this->_haveIndex('cache_id_expire_index') ||
            !$this->_checkStructureVersion()) {
            $this->_buildStructure();
            if (!$this->_checkStructureVersion()) {
                // Try deleting the file entirely
                if (is_file($this->_options['cache_db_complete_path']))
                {
                    unlink($this->_options['cache_db_complete_path']);
                }
                $this->_db = null;
                $db = $this->_getConnection(); // should reconnect/create a new database

                if (!$this->_checkStructureVersion()) {
                    $this->_repairing = false;
                    Zend_Cache::throwException("Impossible to build cache structure in " . $this->_options['cache_db_complete_path']);
                }
            }
        }
        $this->_repairing = false;
        return true;
    }

    private function _haveTable($table, $columns)
    {
        $ret = $this->_haveCollection('table', $table);
        if (!$ret)
        {
            return false;
        }

        // Check the columns
        $columns = array_flip($columns);
        $r = $this->_query("pragma table_info(?)", array($name));
        while($res = $result->fetchArray())
        {
            unset($columns[$res['name']]);
        }

        if (count($columns))
        {
            // missing columns
            return false;
        }

        return $ret;
    }

    private function _haveIndex($index)
    {
        return $this->_haveCollection('index', $index);
    }
    
    private function _haveCollection($type, $name)
    {
        $result = $this->_query("SELECT * FROM dbname.sqlite_master WHERE type=? AND name=?", array($type, $name));
        if (!$result)
        {
            return false;
        }

        $row = @$result->fetchArray();
        if (!$row || $row['type'] != $type || $row['name'] != $name)
        {
            return false;
        }
    }
    
    /**
     * Deal with the automatic vacuum process
     *
     * @return void
     */
    private function _automaticVacuum()
    {
        if ($this->_options['automatic_vacuum_factor'] > 0) {
            $rand = rand(1, $this->_options['automatic_vacuum_factor']);
            if ($rand == 1) {
                $this->_query('VACUUM');
            }
        }
    }

    /**
     * Register a cache id with the given tag
     *
     * @param  string $id  Cache id
     * @param  string $tag Tag
     * @return boolean True if no problem
     */
    private function _registerTag($id, $tag) {
        $res = $this->_query("DELETE FROM TAG WHERE name=? AND id=?", array($tag, $id));
        $res = $this->_query("INSERT INTO tag (name, id) VALUES (?, ?)", array($tag, $id));
        if (!$res) {
            $this->_log("Zend_Cache_Backend_Sqlite::_registerTag() : impossible to register tag=$tag on id=$id");
            return false;
        }
        return true;
    }

    /**
     * Build the database structure
     *
     * @return false
     */
    private function _buildStructure()
    {
        $this->_query('DROP INDEX tag_id_index');
        $this->_query('DROP INDEX tag_name_index');
        $this->_query('DROP INDEX cache_id_expire_index');
        $this->_query('DROP TABLE version');
        $this->_query('DROP TABLE cache');
        $this->_query('DROP TABLE tag');
        $this->_query('CREATE TABLE version (num INTEGER PRIMARY KEY)');
        $this->_query('CREATE TABLE cache (id TEXT PRIMARY KEY, content BLOB, lastModified INTEGER, expire INTEGER)');
        $this->_query('CREATE TABLE tag (name TEXT, id TEXT)');
        $this->_query('CREATE INDEX tag_id_index ON tag(id)');
        $this->_query('CREATE INDEX tag_name_index ON tag(name)');
        $this->_query('CREATE INDEX cache_id_expire_index ON cache(id, expire)');
        $this->_query('INSERT INTO version (num) VALUES (1)');
    }

    /**
     * Check if the database structure is ok (with the good version)
     *
     * @return boolean True if ok
     */
    private function _checkStructureVersion()
    {
        $result = $this->_query("SELECT num FROM version");
        if (!$result) return false;
        $row = @$result->fetchArray();
        if (!$row) {
            return false;
        }
        if (((int) $row['num']) != 1) {
            // old cache structure
            $this->_log('Zend_Cache_Backend_Sqlite::_checkStructureVersion() : old cache structure version detected => the cache is going to be dropped');
            return false;
        }
        return true;
    }

    /**
     * Clean some cache records
     *
     * Available modes are :
     * Zend_Cache::CLEANING_MODE_ALL (default)    => remove all cache entries ($tags is not used)
     * Zend_Cache::CLEANING_MODE_OLD              => remove too old cache entries ($tags is not used)
     * Zend_Cache::CLEANING_MODE_MATCHING_TAG     => remove cache entries matching all given tags
     *                                               ($tags can be an array of strings or a single string)
     * Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG => remove cache entries not {matching one of the given tags}
     *                                               ($tags can be an array of strings or a single string)
     * Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG => remove cache entries matching any given tags
     *                                               ($tags can be an array of strings or a single string)
     *
     * @param  string $mode Clean mode
     * @param  array  $tags Array of tags
     * @return boolean True if no problem
     */
    private function _clean($mode = Zend_Cache::CLEANING_MODE_ALL, $tags = array())
    {
        switch ($mode) {
            case Zend_Cache::CLEANING_MODE_ALL:
                $res1 = $this->_query('DELETE FROM cache');
                $res2 = $this->_query('DELETE FROM tag');
                return $res1 && $res2;
                break;
            case Zend_Cache::CLEANING_MODE_OLD:
                $mktime = time();
                $res1 = $this->_query("DELETE FROM tag WHERE id IN (SELECT id FROM cache WHERE expire>0 AND expire<=?)", array($mktime));
                $res2 = $this->_query("DELETE FROM cache WHERE expire>0 AND expire<=?", array($mktime));
                return $res1 && $res2;
                break;
            case Zend_Cache::CLEANING_MODE_MATCHING_TAG:
                $ids = $this->getIdsMatchingTags($tags);
                $result = true;
                foreach ($ids as $id) {
                    $result = $this->remove($id) && $result;
                }
                return $result;
                break;
            case Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG:
                $ids = $this->getIdsNotMatchingTags($tags);
                $result = true;
                foreach ($ids as $id) {
                    $result = $this->remove($id) && $result;
                }
                return $result;
                break;
            case Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG:
                $ids = $this->getIdsMatchingAnyTags($tags);
                $result = true;
                foreach ($ids as $id) {
                    $result = $this->remove($id) && $result;
                }
                return $result;
                break;
            default:
                break;
        }
        return false;
    }

    /**
     * Check if the database structure is ok (with the good version), if no : build it
     *
     * @throws Zend_Cache_Exception
     * @return boolean True if ok
     */
    private function _checkAndBuildStructure()
    {
        if (!($this->_structureChecked)) {
            if (!$this->_checkStructureVersion()) {
                $this->_buildStructure();
                if (!$this->_checkStructureVersion()) {
                    Zend_Cache::throwException("Impossible to build cache structure in " . $this->_options['cache_db_complete_path']);
                }
            }
            $this->_structureChecked = true;
        }
        return true;
    }

}

