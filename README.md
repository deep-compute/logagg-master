# logagg-master
**Master service and CLI for logagg**

Administration and cluster-management of logagg for Admins and users respectively.

----------
## Prerequisites
* Python >= 3.5
----------
## Components/Architecture/Terminology
* `mongoDB` : Storage where all the information of components for master is stored.
* `nsq` : The central location where logs are sent by `collector`(s) after formatting, as messages.
* `nsq-api`: A service that lets master read messages. 

----------
## Installation

#### Install the logagg_master package where you want to run the `logagg-master` service 
```bash
pip3 install git+https://github.com/supriyopaul/logagg_master.git
```
#### Install nsq_api package to read messages from nsq
```bash
pip2 install git+https://github.com/supriyopaul/nsq_api.git
```
#### Bring a `mongoDB` database instance up for running `logagg-master` service

- Install [`mongoDB`](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-linux/)
- Create user for `mongoDB` using the following commands:
    ```bash
    $ mongo
    .
    .
    2018-03-01T03:47:54.027-0800 I CONTROL  [initandlisten] 
    > use admin
    > db.createUser(
    ...    {
    ...      user: "<username>",
    ...      pwd: "<passwd>",
    ...      roles: [ "readWrite", "dbAdmin" ]
    ...    }
    ... )
    Successfully added user: { "user" : "<username>", "roles" : [ "readWrite", "dbAdmin" ] }
    ```
----------
## Run `logagg-master` service
* **Help**
    ```bash
    logagg-master runserver --help
    ```
* **Command to run `logagg-master` service**
     - [Full API doccumentation](https://) of `logagg-master` service
    ```bash
    logagg-master runserver --host <ip/DNS> --port <port_number> --auth key=<key>:secret=<passwd> --mongodb host=<mongoDB_host>:port=<mongoDB_port>:user=<mongoDB_username>:passwd=<mongoDB_password>:db=<DBname>
    ```

* **Command to run nsq-api service**
    - *Note*: Multiple nsq-api(s) are supported per `logagg-master`
    ```bash
    nsq-api runserver --host <ip/DNS> --port <port_number> --master host=<master_host>:port=<master_port>:key=<master_auth_key>:secret=<master_auth_secret>
    ```

## Commands for logagg-cli
Command line interface for logagg.
### Master commands
Commands require master details along with **auth-key** and **auth-secret** to be saved.
* **help**
    ```bash
    logagg-cli master --help
        usage: logagg-cli master [-h] {add,list,nsq} ...

        positional arguments:
          {add,list,nsq}
            add           Store logagg-master details
            list          Print logagg-master details
            nsq           NSQ for logagg-master

        optional arguments:
          -h, --help      show this help message and exit
    ```
* **master add**
    Store `logagg-master` details
    ```bash
    logagg-cli master add --host <master_host> --port <master_> --auth key=<master_key>:secret=<master_secret>
    ```
* **master list**
    Print `logagg-master` details
    ```bash
    logagg-cli master list
    ```
* **master nsq add**
    Add NSQ details for `logagg-master`.
    Before adding NSQ details make sure to [Install](http://nsq.io/deployment/installing.html) the `nsq` package, at where we need to bring up the `nsq` server.
        - Run the following commands to **install** `nsq`:
        ```BASH
        $ sudo apt-get install libsnappy-dev
        $ wget https://s3.amazonaws.com/bitly-downloads/nsq/nsq-1.0.0-compat.linux-amd64.go1.8.tar.gz
        $ tar zxvf nsq-1.0.0-compat.linux-amd64.go1.8.tar.gz
        $ sudo cp nsq-1.0.0-compat.linux-amd64.go1.8/bin/* /usr/local/bin
        ```
        - Run the `nsq` instances at the required server with following commands:
        **NOTE:** Multiple nsq instances are supported
            ```bash
            nsqlookupd
            nsqd -lookupd-tcp-address <ip/DNS>:4160
            nsqadmin -lookupd-http-address <ip/DNS>:4161
            ```
    ```bash
    logagg-cli master nsq add --nsqd-tcp-address <ip/DNS>:4150 --nsqd-http-address <ip/DNS>:4151
    ```
* **master nsq list**
    Print NSQ details for `logagg-master`
    ```bash
    logagg-cli master nsq list
    ```
### Cluster commands
Does **not** require admin permissions to `logagg-master`.
* **help**
    ```bash
    logagg-cli cluster --help
    usage: logagg-cli cluster [-h]
                              {create,list,join,use,delete,change-password,components,tail}
                              ...

    positional arguments:
      {create,list,join,use,delete,change-password,components,tail}
        create              Create a cluster
        list                List all the clusters in master
        join                Join an existing cluster
        use                 Use an existing cluster
        delete              Delete an existing cluster info from local list of
                            clusters
        change-password     Change password of an existing cluster
        components          List all the components in cluster
        tail                Tail logs in cluster

    optional arguments:
      -h, --help            show this help message and exit
    ```
* **cluster create**
    Add a cluster to `logagg-master` so that components like `collectors` can send logs.
    ```bash
    logagg-cli cluster create --cluster-name <name>
    ```
* **cluster list**
    List all the clusters in `logagg-master`
    ```bash
    logagg-cli cluster list
    ```
* **cluster tail**
    Tail all the logs collected by `logagg-collector`(s)
    ```bash
    logagg-cli cluster tail
    ```
