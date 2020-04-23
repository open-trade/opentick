# OpenTick

![OpenTrade Logo](https://github.com/opentradesolutions/opentrade/blob/master/imgs/ot.png)

OpenTick is a fast tick database for financial timeseries data, built on [FoundationDB](https://www.foundationdb.org/) with simplified SQL layer. 

# Features:
* Built-in price adjustment support
* Nanosecond support
* Python, C++ and Go SDK
* Both sync and async query
* Implicit SQL statement prepare
* Permission Control, check [Python API](https://github.com/opentradesolutions/opentick/blob/master/bindings/python/opentick/client.py) for related functions, turned off by default

* Cache

# Installation on Ubuntu

You need to use **Go >=1.11** which has module support.

```bash
sudo apt install -y python
wget https://www.foundationdb.org/downloads/6.2.15/ubuntu/installers/foundationdb-server_6.2.15-1_amd64.deb
wget https://www.foundationdb.org/downloads/6.2.15/ubuntu/installers/foundationdb-clients_6.2.15-1_amd64.deb
sudo dpkg -i foundationdb-clients_6.2.15-1_amd64.deb foundationdb-server_6.2.15-1_amd64.deb
git clone https://github.com/opentradesolutions/opentick
make build
sudo apt install nodejs
sudo npm install -g pm2
pm2 start ./opentick
```

**Note:** FoundationDB runs in memory storage mode and only one process by default. You can change it to disk storage as belows:
```bash
user@host$ fdbcli
fdb> configure ssd
```

Fore more configuration on FoundationDB, please check [FoundationDB Configuration](https://apple.github.io/foundationdb/configuration.html)

# Usage

[Python](https://github.com/opentradesolutions/opentick/blob/master/bindings/python/test.py)
```bash
pip install opentick
```

[C++](https://github.com/opentradesolutions/opentick/blob/master/bindings/cpp/test.cc)

[Go](https://github.com/opentradesolutions/opentick/blob/master/bindings/go/test.go)

# Performance

100k ohlcv bar inserted in 1 second.

```bash
user@host:~/opentick/bindings/go$ go run test.go
2018/11/27 21:27:23 4.500470184s 5.500314708s 0 100000 all insert futures get done
2018/11/27 21:27:25 861.306778ms 1.139363333s 0 10 all batch insert futures get done
2018/11/27 21:27:26 805.542584ms 100000 retrieved with ranges
2018/11/27 21:27:27 1.782497936s 100000 retrieved with async
2018/11/27 21:27:29 1.424262818s 100000 retrieved with one sync
```

```bash
user@host:~/opentick/bindings/python$ ./test.py
2018-11-27 21:29:10.168138 0:00:00.200577 0:00:06.724991 0 100000 all insert futures get done
2018-11-27 21:29:12.192570 0:00:00.176540 0:00:00.959563 0 10 all batch insert futures get done
2018-11-27 21:29:13.460025 0:00:01.267462 100000 retrieved with ranges
2018-11-27 21:29:15.077686 0:00:01.617666 100000 retrieved with async
2018-11-27 21:29:16.777043 0:00:01.699361 100000 retrieved with one sync
```

```bash
user@host:~/opentick/bindings/cpp$ make test
21:33:19.231156889: 4.22207s 4.84954s 0 100000 all insert futures get done
21:33:20.172744180: 0.447708s 0.934337s 0 10 all batch insert futures get done
21:33:21.677161076: 1.49497s 100000 retrieved with async
```

# Sample Code (C++)

* **Create database and table**
```C++
auto conn = Connection::Create("127.0.0.1", 1116);
conn->Start();
conn->Execute("create database if not exists test");
conn->Use("test");
conn->Execute(R"(
      create table if not exists test(sec int, interval int, tm timestamp,
      open double, high double, low double, close double, v double, vwap
      double, primary key(sec, interval, tm))
)");
```

* **Execute**
```C++
// opentick prepares the sql statement automatically, no need to prepare explicitly
auto fut = conn->ExecuteAsync(
          "select * from test where sec=1 and interval=?", Args{1}));
auto res = fut->Get(); // blocked wait until execution done
// Get last 2 rows ordering by primary key
auto res = conn->Execute(
        "select tm from test where sec=1 and interval=? limit -2", Args{1});
```

* **Insert**
```C++
static const std::string kInsert =
    "insert into test(sec, interval, tm, open, high, low, close, vol, vwap) "
    "values(?, ?, ?, ?, ?, ?, ?, ?, ?)";
std::vector<Future> futs;
for (auto i = 0; i < 1000; ++i) {
  futs.push_back(conn->ExecuteAsync(kInsert, Args{1, 1, system_clock::now(), 2.2, 2.4, 2.1, 2.3, 1000000, 2.25}));
}
// wait for all insertion done
for (auto fut : futs) fut->Get();
```

* **Batch Insert**
```C++
Argss argss;
for (auto i = 0; i < 1000; ++i) {
  argss.push_back(Args{1, i, system_clock::now(), 2.2, 2.4, 2.1, 2.3, 1000000, 2.25});
}
conn->BatchInsert(kInsert, argss);
```

* **Price Adjustments**

```C++
auto res = conn->Execute(
        "select tm, adj(open), adj(high), adj(low), adj(close), adj(vol) from test where sec=1 and interval=? limit -2", Args{1});
```

For more details, please checkout [adj_test.go](https://github.com/opentradesolutions/opentick/blob/master/adj_test.go)
