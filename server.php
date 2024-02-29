<?php
declare(strict_types=1);
header("Content-type:text/html;charset=UTF-8");

use Swoole\Server as MqttServer;
use Swoole\Runtime as Runtime;

/**
 * Class MQTT Server
 */
class Server
{
    /**
     * @var MqttServer
     */
    private $mqttServer;

    /**
     * MqttServer host
     * @var string
     */
    private $host='0.0.0.0';

    /**
     * MqttServer port
     * @var int
     */
    private $port=9502;

    public function __construct()
    {
        //创建MQTT Server对象, 监听0.0.0.0:9502端口
        $this->mqttServer = new MqttServer($this->host, $this->port, SWOOLE_BASE);
        $this->mqttServer->set([
            'open_mqtt_protocol' => true, //启用 MQTT 协议
            'worker_num' => swoole_cpu_num() + 1,
            'max_request' => 100000,
            'dispatch_mode' => 2,
            'daemonize' => false
        ]);

        //MQTT Server监听连接进入事件
        $this->mqttServer->on('Connect', function ($server, int $fd) {
            echo sprintf('【%s】MQTT Client: Connect, ClientId:【%s】%s', date('Y-m-d H:i:s'), $fd, PHP_EOL);
        });

        //MQTT Server监听数据接收事件
        $this->mqttServer->on('Receive', function ($server, int $fd, $reactor_id, string $data) {
            echo sprintf('【%s】MQTT ClientId:【%s】, Received Data:【%s】, Received Length【%s】%s', date('Y-m-d H:i:s'), $fd, $data, strlen($data), PHP_EOL);
            //设备业务分发
            Dispatcher::getInstance()->setMqttServer($server)->dispatch($fd, $data);
        });

        //MQTT Server监听连接关闭事件
        $this->mqttServer->on('Close', function ($server, int $fd) {
            echo sprintf('【%s】MQTT Client: Close, ClientId:【%s】%s', date('Y-m-d H:i:s'), $fd, PHP_EOL);
            //设备客户端断开下线
            Dispatcher::getInstance()->setOffline($fd);
        });

        //启动MQTT服务器
        $this->mqttServer->start();
    }
}

new Server();


/**********************************************************************************************************************************************************************
 *********************************************************************  MQTT Server分发服务业务类  **********************************************************************
 *********************************************************************************************************************************************************************/


class Dispatcher
{
    /** @var int  */
    const ONLINE=1;
    /** @var int  */
    const OFFLINE=0;
    /** @var string  */
    const DEVINFO='devinfo';
    /** @var string  */
    const DATA='data';

    /**
     * MQTT Server
     * @var object
     */
    private $mqttServer;
    /**
     * Singleton instance
     * @var object
     */
    private static $instance;

    /**
     * Mysql
     * @var object
     */
    private $mysql;
    /**
     * Mysql dsn
     * @var string
     */
    private $dsn;
    /**
     * Mysql host
     * @var string
     */
    private $host="220.180.237.48:16306";
    /**
     * Mysql DB port
     * @var int
     */
    private $port=16306;
    /**
     * Mysql username
     * @var string
     */
    private $username='root';
    /**
     * Mysql password
     * @var string
     */
    private $password='Scrm789456123';
    /**
     * Mysql database
     * @var string
     */
    private $db='radarIot';
    /**
     * Mysql sql text
     * @var null
     */
    private $sql=null;
    /**
     * log msg output
     * @var string
     */
    private $outputMsg;

    /**
     * 防止外部实例化
     * Constructor
     */
    private function __construct()
    {
        Runtime::enableCoroutine(SWOOLE_HOOK_ALL);
        $this->connect();//mysql数据库连接
    }

    /**
     * PDO数据库连接
     * @return void
     */
    private function connect()
    {
        $this->dsn = sprintf('mysql:host=%s;port=%d;dbname=%s;charset=utf8mb4', $this->host, $this->port, $this->db);
        try {
            $this->mysql = new PDO($this->dsn, $this->username, $this->password, [PDO::ATTR_PERSISTENT => true]);
            $this->mysql->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (Exception $exception) {
            echo sprintf('【%s】Database Connect Error:【%s】%s', date('Y-m-d H:i:s'), $exception->getMessage(), PHP_EOL);
            exit;
        }
    }

    /**
     * 析构方法销毁PDO连接
     * Destructor
     */
    public function __destruct()
    {
        unset($this->mysql);
    }

    /**
     * 防止外部克隆
     * @return void
     */
    private function __clone()
    {

    }

    /**
     * 获取实例方法
     * @return self
     */
    public static function getInstance()
    {
        if (!(self::$instance instanceof self)) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    /**
     * 设置MQTT Server服务
     * @param $server
     * @return $this
     */
    public function setMqttServer($server)
    {
        $this->mqttServer = $server;

        return $this;
    }

    /**
     * 监听数据接收事件
     * 进行分发记录
     * @param int $clientId
     * @param string $data
     * @return void
     */
    public function dispatch(int $clientId, string $data)
    {
        $this->outputMsg = sprintf('【%s】心跳呼吸记录仪设备MQTT分发服务, ', date('Y-m-d H:i:s'));

        /** @var  $header */
        $header = $this->mqttGetHeader($data);
        /** @var  $deviceData */
        $deviceData = ['header' => $header];
        $this->outputMsg .= sprintf('MQTT ClientId:【%s】, Received Data:【%s】, Output Header Data【%s】, ', $clientId, $data, serialize($header));
        if ($header['type'] == 1) {
            /** @var  $response */
            $response = chr(32) . chr(2) . chr(0) . chr(0);
            $deviceData['response'] = $response;
            $this->eventConnect($header, substr($data, 2));
            $this->mqttServer->send($clientId, $response);
            $this->outputMsg .= sprintf('MQTT Server Send Response:【%s】', $response);
        } elseif ($header['type'] == 3) {
            /** @var  $offset */
            $offset = 2;
            /** @var  $topic */
            $topic = $this->decodeString(substr($data, $offset));
            $deviceData['topic'] = $topic;
            $offset += strlen($topic) + 2;
            /** @var  $msg */
            $msg = substr($data, $offset);
            $deviceData['msg'] = $msg;
            $this->outputMsg .= sprintf('Client Msg:【Topic:%s】, 【Msg:%s】, ', $topic, $msg);

            try {
                /** @var  $data */
                $data = (array)json_decode($msg);
                switch ($topic) {
                    case self::DEVINFO:
                        if (isset($data['id']) && $this->mqttServer->exist($clientId)) {
                            $this->_initDevice($clientId, $data['id']);
                        }

                        break;
                    case self::DATA:
                        if (isset($data['hr'], $data['br']) && $this->mqttServer->exist($clientId)) {
                            $this->_initDeviceIndex($clientId, $data);
                        }

                        break;
                }
                $this->mqttServer->send($clientId, 'mqtt server dispatched'.PHP_EOL);
            } catch (\Exception $exception) {
                $this->outputMsg .= sprintf('报错信息:【%s】', $exception->getMessage());
            }
        }
        //接收客户端数据保存
        $this->_initDeviceData($clientId, serialize($deviceData));

        echo $this->outputMsg, PHP_EOL;
    }

    /**
     * Get MQTT Header
     * @param string $data
     * @return array
     */
    protected function mqttGetHeader(string $data):array
    {
        /** @var  $byte */
        $byte = ord($data[0]);
        /** @var  $header */
        $header['type'] = ($byte & 0xF0) >> 4;
        $header['dup'] = ($byte & 0x08) >> 3;
        $header['qos'] = ($byte & 0x06) >> 1;
        $header['retain'] = $byte & 0x01;

        return $header;
    }

    /**
     * Get Event Connect Info
     * @param array $header
     * @param string $data
     * @return  false[]|string[]
     */
    protected function eventConnect(array $header, string $data):array
    {
        /** @var  $connectInfo */
        $connectInfo = ['protocol_name' => $this->decodeString($data)];
        /** @var  $offset */
        $offset = strlen($connectInfo['protocol_name']) + 2;
        $connectInfo['version'] = ord(substr($data, $offset, 1));

        $offset += 1;
        /** @var  $byte */
        $byte = ord($data[$offset]);
        $connectInfo['willRetain'] = ($byte & 0x20 == 0x20);
        $connectInfo['willQos'] = ($byte & 0x18 >> 3);
        $connectInfo['willFlag'] = ($byte & 0x04 == 0x04);
        $connectInfo['cleanStart'] = ($byte & 0x02 == 0x02);
        $offset += 1;

        $connectInfo['keepalive'] = $this->decodeValue(substr($data, $offset, 2));
        $offset += 2;
        $connectInfo['clientId'] = $this->decodeString(substr($data, $offset));

        return $connectInfo;
    }

    /**
     * Decode String
     * @param $data
     * @return false|string
     */
    protected function decodeString($data)
    {
        /** @var  $length */
        $length = $this->decodeValue($data);

        return substr($data, 2, $length);
    }

    /**
     * Decode Value
     * @param $data
     * @return float|int
     */
    private function decodeValue($data)
    {
        return 256 * ord($data[0]) + ord($data[1]);
    }

    /**
     * 校验参数字符串是否设备型号
     * @param string $data
     * @return bool
     */
    private function isDeviceModel(string $data):bool
    {
        return strlen($data) == 15 && is_numeric($data);
    }

    /**
     * 设备客户端断开下线
     * @param int $clientId
     * @return void
     */
    public function setOffline(int $clientId)
    {
        /** @var  $device */
        $device = $this->findDeviceByClientId($clientId);
        if (!!$device && $device['online'] == self::ONLINE) {
            /** @var  $date */
            $date = date('Y-m-d H:i:s');
            $this->sql = sprintf("UPDATE %s SET online=%d,updatedAt=%s WHERE id=%d",
                'opiot_device', self::OFFLINE, "'{$date}'", intval($device['id']));

            $this->insertOrUpdate($this->sql);
        }
    }

    /**
     * 同步设备在线状态
     * @param int $clientId
     * @return void
     */
    protected function setOnline(int $clientId)
    {
        /** @var  $device */
        $device = $this->findDeviceByClientId($clientId);
        if (!!$device && $device['online'] == self::OFFLINE) {
            /** @var  $date */
            $date = date('Y-m-d H:i:s');
            $this->sql = sprintf("UPDATE %s SET online=%d,updatedAt=%s WHERE id=%d",
                'opiot_device', self::ONLINE, "'{$date}'", intval($device['id']));

            $this->insertOrUpdate($this->sql);
        }
    }

    /**
     * 根据设备型号获取设备数据
     * @param string $deviceId
     * @return array|false|null
     */
    protected function findDeviceByDid(string $deviceId)
    {
        /** @var  $sql */
        $sql = sprintf('SELECT %s FROM %s WHERE `deviceNo` = %s',
            'deviceNo,clientId', 'opiot_device', "'$deviceId'");

        return $this->selectRow($sql);
    }

    /**
     * 根据设备客户端TCP唯一标识ID获取设备在线数据
     * @param int $clientId
     * @return array|false|null
     */
    protected function findDeviceByClientId(int $clientId)
    {
        /** @var  $sql */
        $sql = sprintf('SELECT %s FROM %s WHERE `clientId` = %d',
            'id,online', 'opiot_device', $clientId);

        return $this->selectRow($sql);
    }

    /**
     * 根据设备客户端TCP唯一标识ID文件描述符判断设备是否在线
     * @param int $clientId
     * @return bool
     */
    protected function isOnline(int $clientId):bool
    {
        if (!$this->mqttServer->exist($clientId)) {
            return false;
        }
        /** @var  $device */
        $device = $this->findDeviceByClientId($clientId);
        if (!!$device) {
            return boolval($device['online']);
        }

        return false;
    }

    /**
     * 设备参数保存, 设备型号, 客户端唯一标识ID
     * @param int $clientId
     * @param string $deviceNo
     * @return void
     */
    protected function _initDevice(int $clientId, string $deviceNo)
    {
        /** @var  $device */
        $device = $this->findDeviceByDid($deviceNo);
        /** @var  $date */
        $date = date('Y-m-d H:i:s');

        if (!$device) {
            $this->sql = sprintf('INSERT INTO %s (deviceNo, clientId, online, createdAt, updatedAt) VALUES(%s, %d, %d, %s, %s)',
                'opiot_device', "'{$deviceNo}'", $clientId, self::ONLINE, "'{$date}'", "'{$date}'");
        }
        if (isset($device['clientId']) && $clientId != intval($device['clientId'])) {
            $this->sql = sprintf("UPDATE %s SET clientId=%d,online=%d,updatedAt=%s WHERE deviceNo=%s",
                'opiot_device', $clientId, self::ONLINE, "'{$date}'", "'{$deviceNo}'");
        }

        if (!is_null($this->sql)) {
            $this->insertOrUpdate($this->sql);
        }
    }

    /**
     * 设备指数记录
     * 呼吸率指数
     * 心率指数
     * @param int $clientId
     * @param array $data
     * @return void
     */
    protected function _initDeviceIndex(int $clientId, array $data)
    {
        /** @var  $device */
        $device = $this->findDeviceByClientId($clientId);
        if (!$device) {
            return;
        }
        /** @var  $date */
        $date = date('Y-m-d H:i:s');
        $this->sql = sprintf("INSERT INTO %s (deviceId, clientId, breathRate, heartRate, createdAt, updatedAt) VALUES(%d, %d, %d, %d, %s, %s)",
            'opiot_device_index', $device['id'], $clientId, $data['br'], $data['hr'], "'{$date}'", "'{$date}'");

        $this->insertOrUpdate($this->sql);
    }

    /**
     * 接收设备客户端数据保存
     * @param int $clientId
     * @param string $data
     * @return void
     */
    protected function _initDeviceData(int $clientId, string $data)
    {
        $this->outputMsg = sprintf('【%s】MQTT服务接收心跳呼吸记录仪设备客户端数据【clientId: %d, 接收数据: %s】。',
            date('Y-m-d H:i:s'), $clientId, $data);
        /** @var  $date */
        $date = date('Y-m-d H:i:s');
        $this->sql = sprintf("INSERT INTO %s (clientId, data, createdAt, updatedAt) VALUES(%d, %s, %s, %s)",
            'opiot_device_log', $clientId, "'{$data}'", "'{$date}'", "'{$date}'");

        $this->insertOrUpdate($this->sql);
        echo $this->outputMsg, PHP_EOL;
    }

    /**
     * 查询获取单条数据
     * @param string $sql
     * @param int $time
     * @return void
     */
    private function selectRow(string $sql, int $time=1)
    {
        try {
            return $this->mysql->query($sql)->fetch(PDO::FETCH_ASSOC);
        } catch (PDOException $exception) {
            if ($exception->getCode() == 'HY000') {
                $this->connect();//数据库重连
            }
            $time++;

            if ($time <= 6) {
                $this->selectRow($sql, $time);
            }
        }
    }

    /**
     * mysql插入单条数据
     * @param string $sql
     * @param int $time
     * @return bool|void
     */
    private function insertOrUpdate(string $sql, int $time=1)
    {
        $this->outputMsg .= sprintf('执行SQL语句:【%s】, ', $sql);
        try {
            $rows = $this->mysql->exec($sql);
            $this->outputMsg .= sprintf('执行结果:【%s】。', '成功');

            return !!$rows;
        } catch (PDOException $exception) {
            $this->outputMsg .= sprintf('执行结果:【%s】。', $exception->getMessage());
            if ($exception->getCode() == 'HY000') {
                $this->connect(); //数据库重连
            }
            $time++;

            if ($time <= 6) {
                $this->insertOrUpdate($sql, $time);
            }
        }
    }
}