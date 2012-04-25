<?
/*
	TODO

	- get the config stuff sorted out, remove these defines and use configuration data passed into AWSDataTool 
	- create validator method that checks all domains and buckets exist, and creates them if not, must be called explicitly
	- delete objects from sdb & s3
	- sort out files & includes per class
	- namespace?
		- php
		- production / dev / staging in S3/SDB names
	- _ vs - in domain & bucket names
	- proper exception chain
	- populate AWSDataObjects in SDB selects - or rather do AWS "search"/"select"s

	- github project
*/
	require_once("AWSSDKforPHP/sdk.class.php");
	require_once("flexihash/include/init.php");


	class AWSDataException		extends Exception {}
	class AWSConfigException	extends Exception {}
	class S3Exception			extends Exception {}
	class SDBException			extends Exception {}


	class AWSDataObject {

		public	$key			= null;
		public	$attributes		= null;
		public	$object_type	= null;

		function __construct($item_key, $attrs = array(), $object_type = null) {
			$this->key			= $item_key;
			$this->s3_data		= array();
			$this->object_type	= $object_type === null ? get_class($this) : $object_type;

			if(null === $attrs) {
				$attrs = array();
			} 

			foreach($attrs as $k => $v) {
				if(strlen($v) === 0) {
					continue;
				}
				if(strlen($v) > 1024) {
					$this->s3_data[$k] = $v;
				}
				else {
					$this->attributes[$k] = $v;
				}
			}

			$this->attributes[AWSDataTool::S3_FIELDLIST_KEY] = json_encode(array_keys($this->s3_data));

			return;

		}

		function __get($name) {
			if(array_key_exists($name, $this->attributes)) {
				return $this->attributes[$name];
			}
			trigger_error("Undefined property " . $this->object_type . "->" . $name);
			return false;
		}

		function is_loaded() {
			return count($this->attributes) > 0;
		}
	}


	class AWSDataTool {

		const	S3_FIELDLIST_KEY			= "s3-fieldlist-key";
		const	SERVICE_NAME_S3				= "S3";
		const	SERVICE_NAME_SDB			= "SDB";
		const	MODEL_TYPE_SEPARATOR_S3		= "-";
		const	MODEL_TYPE_SEPARATOR_SDB	= "_";
		const	MODEL_TYPE_SEPARATOR_NAME	= "MODEL_TYPE_SEPARATOR_";
		const	CONFIG_KEY_S3_BUCKETS		= "s3_bucket_names";
		const	CONFIG_KEY_SDB_DOMAINS		= "sdb_domain_names";
		const	CONFIG_KEY_MODEL			= "model";
		const	CONFIG_KEY_DATA_TYPES		= "data_types";
		const	CONFIG_KEY_SHARDS			= "shards";

		public	$sdbtool		= null;
		public	$s3				= null;
		public	$config_data	= null;
		private	$cfu			= null;
		

		function __construct($config_data) {
			if(!isset($config_data) || null === $config_data) {
				throw new AWSConfigException("You need to supply an array of configuration values to use " . get_class($this) . ".\n");
			}

			$this->config_data = $config_data;

			$data_types = array_keys($this->config_data[$this::CONFIG_KEY_DATA_TYPES]);
			if(!isset($data_types) || null === $data_types) {
				throw new AWSDataException("You need to supply an array of suitably formatted data types to use " . get_class($this) . ".\n");
			}

			$this->sdbtool	= new SDBTool($data_types);
			$this->s3		= new AmazonS3();
			$this->cfu		= new CFUtilities();

			$this->config_data[$this::CONFIG_KEY_S3_BUCKETS]	= $this->create_data_type_array($this::SERVICE_NAME_S3);
			$this->config_data[$this::CONFIG_KEY_SDB_DOMAINS]	= $this->create_data_type_array($this::SERVICE_NAME_SDB);

			print_r($this->config_data);
		}

		function create_data_type_array($service) {
			print "creating $service data type arrays for {$this->config_data[$this::CONFIG_KEY_MODEL]}\n";
			$data_type_array = array();
			foreach(array_keys($this->config_data[$this::CONFIG_KEY_DATA_TYPES]) as $data_type) {
				$separator_name	= $this::MODEL_TYPE_SEPARATOR_NAME . $service;
				$separator		= constant(get_class($this) . "::" . $separator_name);
				for($i=1;$i<=$this->config_data[$this::CONFIG_KEY_DATA_TYPES][$data_type][$this::CONFIG_KEY_SHARDS];$i++) {
					array_push($data_type_array, $this->config_data[$this::CONFIG_KEY_MODEL] . $separator . $data_type . $separator . str_pad($i, 2, "0", STR_PAD_LEFT));
				}
			}

			return $data_type_array;
		}

		function check_sdb_domains() {
			print "Checking SDB domains:\n";
			$existing_domains = $this->sdbtool->get_domain_list();
			print_r($existing_domains);
			print_r($this->config_data[$this::CONFIG_KEY_SDB_DOMAINS]);
			print_r(array_diff($existing_domains, $this->config_data[$this::CONFIG_KEY_SDB_DOMAINS]));
			foreach($this->config_data[$this::CONFIG_KEY_SDB_DOMAINS] as $domain_name) {
				print "- ";
				if(in_array($domain_name, $existing_domains) === false) {
					print "NOT ";
				}
				print "OK: $domain_name\n";
			}
		}

		function check_s3_buckets() {
			print "Checking S3 buckets:\n";
			foreach($this->config_data[$this::CONFIG_KEY_S3_BUCKETS] as $bucket_name) {
				print "- ";
				if(!$this->s3->if_bucket_exists($bucket_name)) {
					print "NOT ";
				}
				print "OK: $bucket_name\n";
			}
		}

		function purge_s3_buckets($buckets = null) {
			if(null == $buckets) {
				$buckets = $this->config_data[$this::CONFIG_KEY_S3_BUCKETS];
			}
			print "Purging S3 buckets:\n";
			//foreach($this->config_data[$this::CONFIG_KEY_S3_BUCKETS] as $bucket_name) {
			foreach($buckets as $bucket_name) {
				print "- ";
				if(false === $this->s3->delete_bucket($bucket_name, true)) { // true flag = force delete all content
					print "NOT OK: $bucket_name - failed to delete\n";
					continue;
				}
				while($this->s3->if_bucket_exists($bucket_name)) {
					sleep(1);
				}
				print "OK: $bucket_name\n";
			}
		}

		function create_s3_buckets($purge = false) {
			if($purge) {
				$this->purge_s3_buckets();
			}
			print "Creating S3 buckets:\n";
			foreach($this->config_data[$this::CONFIG_KEY_S3_BUCKETS] as $bucket_name) {
				print "- ";
				$cbr = $this->s3->create_bucket($bucket_name, AmazonS3::REGION_US_W1);
				if(!$cbr->isOK()) {
					print "NOT OK: $bucket_name - failed to create. Does it already exist?\n";
					continue;
				}
				while(!$this->s3->if_bucket_exists($bucket_name)) {
					sleep(1);
				}
				print "OK: $bucket_name\n";
			}	
		}

		function get_attributes($object_type, $aws_data_key) {
			return $this->get_object($object_type, $aws_data_key)->attributes;
		}

		function get_object($object_type, $aws_data_key) {
			$aws_data_object = new AWSDataObject($aws_data_key, array(), $object_type);
			return $this->populate($aws_data_object);
		}

		function populate($aws_data_object) {
			$retrieve_method	= "retrieve_" . $aws_data_object->object_type;
			try {
				$sdb_retrieve_result			= $this->sdbtool->$retrieve_method($aws_data_object->key);
				$aws_data_object->attributes	= $sdb_retrieve_result[$aws_data_object->key];
				$s3_bucket						= str_replace("_", "-", $sdb_retrieve_result['domain_name']);
				if(array_key_exists(self::S3_FIELDLIST_KEY, $aws_data_object->attributes)) {
					foreach(json_decode($aws_data_object->attributes[self::S3_FIELDLIST_KEY], true) as $s3_field) {
						$s3_key = $aws_data_object->key . "/" . $s3_field;
						$s3_url = $this->s3->get_object_url($s3_bucket, $s3_key, '5 minutes');
						print "trying to retrieve {$s3_key} from {$s3_bucket} at $s3_url: \n";
						$aws_data_object->attributes[$s3_field] = file_get_contents($s3_url);
					}
				}
				unset($aws_data_object->attributes[self::S3_FIELDLIST_KEY]);
				return $aws_data_object;
			}
			catch(Exception $e) {
				print "Error retrieving {$aws_data_object->object_type} {$aws_data_object->key}: {$e->getMessage()}\n";
				return false;
			}
		}

		function store($aws_data_object) {
			$store_method	= "store_" . $aws_data_object->object_type;
			try {
				$sdb_store_result	= $this->sdbtool->$store_method($aws_data_object->key, $aws_data_object->attributes);
				$s3_bucket			= str_replace("_", "-", $sdb_store_result['domain_name']);

				foreach($aws_data_object->s3_data as $attribute_name => $data) {
					$s3_key	= $aws_data_object->key . "/" . $attribute_name;		
					print "trying to store {$s3_key} in {$s3_bucket}:";
					$response = $this->s3->create_object($s3_bucket, $s3_key, array('body' => $data));
					if(!$response->isOK()) {
						throw new S3Exception("ERROR: Result of S3 call: {$response->body->Message}\n");
					}
					print "succeeded.\n";
				}
				return $sdb_store_result['domain_name'];
			}
			catch(SDBException $sdbe) {
				print "SDBException: Error storing {$aws_data_object->object_type} {$aws_data_object->key}: {$sdbe->getMessage()}\n";
			}
			// TODO sort out exception handling hierarchy
			catch(S3Exception $s3e) {
				print "S3Exception: Error storing {$aws_data_object->object_type} {$aws_data_object->key}: {$s3e->getMessage()}\n";
			}
		}

		function remove($aws_data_object) {
			// TODO remove SDB item & any associated S3 content
		}
	}


	class SDBTool {

		static function create_padded_domain_names($item_type, $count) {
			$domain_names = array();
			for($i=1;$i<=$count;$i++) {
				array_push($domain_names, $item_type . "_" . str_pad($i, 2, "0", STR_PAD_LEFT));
			}
			return $domain_names;
		}

		public	$sdb	= null;	// SimpleDB object
		private	$sdb_r	= null;	// ReflectionClass for SimpleDB object
		public	$fh		= null;	// Flexihash array
		private	$aws_t	= null; // Parent AWSDataTool object for config data

		function __construct($sdb_domain_type_list = null) {

			// i like putting this in _SERVER as it can be executed with 'env AWS_REGION=XXX'
			// when we go to mongrel2 we might need to change this
			if(!array_key_exists('AWS_REGION', $_SERVER)) {
				$_SERVER['AWS_REGION']	= "DEFAULT_URL";
			}

			$this->sdb		= new AmazonSDB();
			$this->sdb_r	= new ReflectionClass($this->sdb);
			$aws_constants	= ($this->sdb_r->getConstants());
			$aws_region		= $aws_constants[$_SERVER['AWS_REGION']];
			$this->sdb->set_region($aws_region);

			if($sdb_domain_type_list === null) {
				file_put_contents("php://stderr", "\033[0;33mInitialising " . __CLASS__ . " with no SDB types, as SDB tool only.\033[m\n");
				return;
			}

			$this->fh		= array();

			foreach($sdb_domain_type_list as $sdb_domain_type) {
				$label	= "NUMBER_AWS_DOMAINS_" . strtoupper($sdb_domain_type);
				// let's have at least one domain per type we want to store
				if(!defined($label)) { define($label, 1); }
				// add hasher for this type
				if(!array_key_exists($sdb_domain_type, $this->fh)) {
					$this->fh[$sdb_domain_type] = new Flexihash();
				}
				// add targets for this type's hasher
				foreach(SDBTool::create_padded_domain_names($sdb_domain_type, constant($label)) as $target_domain) {
					$this->fh[$sdb_domain_type]->addTarget($target_domain);
				}
			}

		}

		function __call($name, $arguments) {

			// try to store, retrieve or select
			if(preg_match("/^(store|retrieve|select)_([A-za-z_]+)$/", $name, $method_name_matches)) {

				$method_name	= $method_name_matches[1];
				$item_type		= $method_name_matches[2];

				// only work with item types we have initialized
				if(array_key_exists($item_type, $this->fh)) {
					switch($method_name) {
						case "store":
							// store_<item_type> (key, keypair_array)
							if(is_array($arguments) && count($arguments) == 2 && is_array($arguments[1])) {
								return $this->store_item($item_type, $arguments[0], $arguments[1]);
							}
							break;

						case "retrieve":
							// retrieve_<item_type> (key), retrieve_<item_type> (key, attributes_to_retrieve)
							if(is_array($arguments)) {
								return $this->retrieve_item($item_type, $arguments[0], $arguments[1]);
							}
							break;

						case "select":
							// select_<item_type> (qualifier_array = null)
							if(is_array($arguments)) {
								return $this->select_items($item_type, $arguments[0]);
							}
							break;
					}
				}

				throw new ReflectionException(__CLASS__ . ": unable to $method_name items of type $item_type");
			}

			// fall back to SDB methods
			if($this->sdb_r->hasMethod($name)) {
				$method = $this->sdb_r->getMethod($name);
				if(is_array($arguments)) {
					return $method->invokeArgs($this->sdb, $arguments);
				}
				return $method->invoke($this->sdb, $arguments);
			}

			// jack it in
			throw new ReflectionException("Neither " . __CLASS__ . " nor its SimpleDB instance has a method $name");
		}

		function store_item($item_type, $item_key, $attribute_keypairs) {
	
			$domain_name	= $this->fh[$item_type]->lookup($item_key);
			$response		= $this->sdb->put_attributes($domain_name, $item_key, $attribute_keypairs); //, $replace = null, $opt = null)
	
			if(!$response->isOK()) {
				throw new SDBException( (string)$response->body->Errors->Error->Code . "\n" . (string)$response->body->Errors->Error->Message . "\n" );
			}
	
			return array(
				"item_key"		=> $item_key,
				"domain_name"	=> $domain_name
			);
		}

		function retrieve_item($item_type, $item_key, $attribute_list = null) {
	
			$domain_name	= $this->fh[$item_type]->lookup($item_key);
			$response		= $this->sdb->get_attributes(
				$domain_name,
				$item_key,
				$attribute_list
			); //, $opt = null)

			if(!$response->isOK()) {
				throw new SDBException( (string)$response->body->Errors->Error->Code . "\n" . (string)$response->body->Errors->Error->Message . "\n" );
			}
	
			$attributes = array();
			foreach($response->body->GetAttributesResult->Attribute as $attr) {
				$attributes[(string)$attr->Name] = (string)$attr->Value;
			}
	
			ksort($attributes);
			return array(
				$item_key		=> $attributes,
				'domain_name'	=> $domain_name
			);
		}

		function select_items($item_type, $qualifier = null) {
			$label				= "NUMBER_AWS_DOMAINS_" . strtoupper($item_type);
			$num_aws_domains	= constant($label);

			for($i = 1; $i <= $num_aws_domains; $i++) {
				$domain_name = $item_type . "_" . str_pad($i, 2, "0", STR_PAD_LEFT);
				$select_query = "SELECT * FROM `{$domain_name}`";
				if(is_array($qualifier)
					&& array_key_exists('field', $qualifier)
					&& array_key_exists('comparator', $qualifier)
					&& array_key_exists('value', $qualifier)) {
					$select_query .= " WHERE `{$qualifier['field']}` {$qualifier['comparator']} '{$qualifier['value']}'";
				}
				$this->sdb->batch()->select($select_query);
			}

			$responses = $this->sdb->batch()->send();

			if(!$responses->areOK()) {
				throw new SDBException( (string)$response->body->Errors->Error->Code . "\n" . (string)$response->body->Errors->Error->Message . "\n" );
			}

			$items = array();
			foreach($responses->getArrayCopy() as $response) {
				$sdb_item = $response->body->Item();
				if(!$sdb_item){ 
					continue;
				}
				foreach($sdb_item as $item) {
					$items[(string)$item->Name] = array();
					foreach($item->Attribute as $attribute) {
						$items[(string)$item->Name][(string)$attribute->Name] = (string)$attribute->Value;
					}
				}
			}

			ksort($items);
			return $items;
		}

	}
?>
