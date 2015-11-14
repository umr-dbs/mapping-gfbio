<?php


class DB {

	public static $db = null;

	static private function connect() {
		if (self::$db)
			return;

		// dbname = 'idessa' user = 'idessa' password = 'idessa'
		// pgsql:host=localhost;port=5432;dbname=testdb;user=bruce;password=mypass
		self::$db = new PDO('pgsql:host=localhost;port=5432;dbname=idessa', 'idessa', 'idessa');
		//self::$db = new PDO('pgsql:host=***REMOVED***;dbname=webusers', 'webusers', 'idessa');
		self::$db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		self::$db->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
		self::$db->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_OBJ); // TODO: check FETCH_CLASS!
	}

	static public function query($sql/*, ...*/) {
		self::connect();

		$stmt = self::$db->prepare($sql);
		$params = func_get_args();
		array_shift($params); // remove $sql
		$stmt->execute($params);
		return $stmt->fetchAll();
	}

	static public function exec($sql/*, ... */) {
		self::connect();

		$stmt = self::$db->prepare($sql);
		$params = func_get_args();
		array_shift($params); // remove $sql
		$stmt->execute($params);
		return $stmt->rowCount();
	}

	static public function beginTransaction() {
		self::$db->beginTransaction();
	}

	static public function commit() {
		self::$db->commit();
	}

	static public function getLastInsertedId($sequenceName) {
		return self::$db->lastInsertId($sequenceName);
	}

}
