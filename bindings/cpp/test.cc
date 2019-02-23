#include "opentick.h"

#include <ctime>
#include <iostream>

using namespace opentick;
using namespace std::chrono;

#define LOG(msg)                                                           \
  do {                                                                     \
    auto d =                                                               \
        duration_cast<nanoseconds>(system_clock::now().time_since_epoch()) \
            .count();                                                      \
    time_t t = d / 1000000000;                                             \
    char buf[80];                                                          \
    strftime(buf, sizeof(buf), "%H:%M:%S", localtime(&t));                 \
    auto nsec = d % 1000000000;                                            \
    sprintf(buf + strlen(buf), ".%09ld", nsec);                            \
    std::cerr << buf << ": " << msg << std::endl;                          \
  } while (0)

static const std::string kInsert =
    "insert into test(sec, interval, tm, open, high, low, close, vol, vwap) "
    "values(?, ?, ?, ?, ?, ?, ?, ?, ?)";

int main() {
  auto conn = Connect("127.0.0.1", 1116);
  LOG("connected");
  conn->Execute("create database if not exists test");
  conn->Use("test");
  auto res = conn->Execute(R"(
      create table if not exists test(sec int, interval int, tm timestamp, 
      open double, high double, low double, close double, vol double, vwap 
      double, primary key(sec, interval, tm))
      )");
  res = conn->Execute("delete from test where sec=?", Args{1});
  LOG("records deleted");
  auto tm = system_clock::now();
  decltype(tm) tm2;
  for (auto i = 0; i < 100; ++i) {
    auto n1 = 10;
    auto n2 = 10000;
    std::vector<Future> futs;
    auto now = system_clock::now();
    for (auto j = 0; j < n1; ++j) {
      for (auto k = 0; k < n2; ++k) {
        auto ms = j * n2 + k;
        tm2 = tm + microseconds(ms);
        futs.push_back(conn->ExecuteAsync(
            kInsert, Args{1, i, tm2, 2.2, 2.4, 2.1, 2.3, 1000000, 2.25}));
      }
    }
    auto now2 = system_clock::now();
    auto diff = duration_cast<microseconds>(now2 - now).count() / 1e6;
    LOG(diff << " async done");
    for (auto f : futs) f->Get();
    auto now3 = system_clock::now();
    auto diff2 = duration_cast<microseconds>(now3 - now2).count() / 1e6;
    diff = duration_cast<microseconds>(now3 - now).count() / 1e6;
    LOG(diff2 << "s " << diff << "s " << i << ' ' << futs.size()
              << " all insert futures get done");
    futs.clear();
    now = system_clock::now();
    for (auto j = 0; j < n1; ++j) {
      Argss argss;
      for (auto k = 0; k < n2; ++k) {
        auto ms = j * n2 + k;
        tm2 = tm + microseconds(ms);
        argss.push_back(Args{1, i, tm2, 2.2, 2.4, 2.1, 2.3, 1000000, 2.25});
      }
      // the batch size is limited by foundationdb transaction size
      //  https://apple.github.io/foundationdb/known-limitations.html
      futs.push_back(conn->BatchInsertAsync(kInsert, argss));
    }
    now2 = system_clock::now();
    diff = duration_cast<microseconds>(now2 - now).count() / 1e6;
    LOG(diff << " async done");
    for (auto f : futs) f->Get();
    now3 = system_clock::now();
    diff2 = duration_cast<microseconds>(now3 - now2).count() / 1e6;
    diff = duration_cast<microseconds>(now3 - now).count() / 1e6;
    LOG(diff2 << "s " << diff << "s " << i << ' ' << futs.size()
              << " all batch insert futures get done");
    auto res = conn->Execute(
        "select tm from test where sec=1 and interval=? and tm=?", Args{i, tm});
    assert(std::get<Tm>((*res)[0][0]) == tm);
    res = conn->Execute(
        "select tm from test where sec=1 and interval=? limit -2", Args{i});
    assert(res->size() == 2);
    assert(std::get<Tm>((*res)[0][0]) == tm2);
    futs.clear();
    now = system_clock::now();
    for (auto j = 0; j <= i; ++j) {
      futs.push_back(conn->ExecuteAsync(
          "select * from test where sec=1 and interval=?", Args{j}));
    }
    ResultSet::element_type res2;
    for (auto f : futs) {
      auto tmp = f->Get();
      if (tmp) {
        res2.insert(res2.end(), tmp->begin(), tmp->end());
      }
    }
    now2 = system_clock::now();
    diff = duration_cast<microseconds>(now2 - now).count() / 1e6;
    LOG(diff << "s " << res2.size() << " retrieved with async");
    std::cerr << std::endl;
  }
  return 0;
}
