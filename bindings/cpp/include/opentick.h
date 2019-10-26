#ifndef OPENTICK_CONNECTION_H_
#define OPENTICK_CONNECTION_H_

#include <atomic>
#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/endian/conversion.hpp>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "json.hpp"

namespace opentick {

using json = nlohmann::json;
using namespace std::chrono;

#define LOG(msg)                                                           \
  do {                                                                     \
    auto d =                                                               \
        duration_cast<nanoseconds>(system_clock::now().time_since_epoch()) \
            .count();                                                      \
    time_t t = d / 1000000000;                                             \
    char buf[80];                                                          \
    strftime(buf, sizeof(buf), "%Y%m%d %H:%M:%S", localtime(&t));          \
    auto nsec = d % 1000000000;                                            \
    sprintf(buf + strlen(buf), ".%09ld", nsec);                            \
    std::cerr << buf << ": " << msg << std::endl;                          \
  } while (0)

struct Logger {
  typedef std::shared_ptr<Logger> Ptr;
  virtual void Info(const std::string& msg) noexcept { LOG(msg); }
  virtual void Error(const std::string& msg) noexcept { LOG(msg); }
};

static inline decltype(auto) Split(const std::string& str, const char* sep,
                                   bool compact = true,
                                   bool remove_empty = true) {
  std::vector<std::string> out;
  boost::split(out, str, boost::is_any_of(sep),
               compact ? boost::token_compress_on : boost::token_compress_off);
  if (remove_empty) {
    out.erase(std::remove_if(out.begin(), out.end(),
                             [](auto x) { return x.empty(); }),
              out.end());
  }
  return out;
}

typedef system_clock::time_point Tm;
typedef std::variant<std::int64_t, std::uint64_t, std::int32_t, std::uint32_t,
                     bool, float, double, std::nullptr_t, std::string, Tm>
    ValueScalar;
typedef std::vector<std::vector<ValueScalar>> ValuesVector;
typedef std::shared_ptr<ValuesVector> ResultSet;
typedef std::variant<ResultSet, ValueScalar> Value;
struct AbstractFuture {
  virtual ResultSet Get(double timeout = 0) = 0;  // timeout in seconds
};
typedef std::shared_ptr<AbstractFuture> Future;
typedef std::vector<ValueScalar> Args;
typedef std::vector<Args> Argss;
typedef std::function<void(ResultSet, const std::string&)> Callback;

class Connection : public std::enable_shared_from_this<Connection> {
 public:
  typedef std::shared_ptr<Connection> Ptr;
  std::string Start() noexcept;
  bool IsConnected() const noexcept { return 1 == connected_; }
  void Login(const std::string& username, const std::string& password,
             const std::string& dbName = "", bool wait = true);
  void Use(const std::string& dbName, bool wait = true);
  Future ExecuteAsync(const std::string& sql, const Args& args = {},
                      Callback callback = {});
  ResultSet Execute(const std::string& sql, const Args& args = {});
  Future BatchInsertAsync(const std::string& sql, const Argss& argss);
  void BatchInsert(const std::string& sql, const Argss& argss);
  int Prepare(const std::string& sql);
  void Close();
  void SetLogger(Logger::Ptr logger) noexcept { logger_ = logger; }
  void SetAutoReconnect(int interval) noexcept { auto_reconnect_ = interval; }

  // add can be host or url like user_name:password@host:port/db_name
  static inline Connection::Ptr Create(const std::string& addr, int port = 0,
                                       const std::string& db_name = "",
                                       const std::string& username = "",
                                       const std::string& password = "",
                                       int timeout = 15) {
    return Connection::Ptr(
        new Connection(addr, port, db_name, username, password, timeout));
  }

 protected:
  Connection(const std::string& addr, int port, const std::string& dbname,
             const std::string& username, const std::string& password,
             int timeout);
  void ReadHead();
  void ReadBody(unsigned len);
  template <typename T>
  void Send(T&& msg);
  void Write();
  void Notify(int, const Value&);
  void AfterConnected(bool sync);

 private:
  int auto_reconnect_ = 0;
  std::atomic<int> connected_ = 0;
  std::string ip_;
  int port_ = 0;
  std::string default_use_;
  std::string username_;
  std::string password_;
  int default_timeout_ = 0;
  std::vector<std::uint8_t> msg_in_buf_;
  std::vector<std::uint8_t> msg_out_buf_;
  std::vector<std::uint8_t> outbox_;
  boost::asio::io_service io_service_;
  boost::asio::io_service::work worker_;
  boost::asio::ip::tcp::socket socket_;
  std::thread thread_;
  std::atomic<int> ticket_counter_ = 0;
  std::condition_variable cv_;
  std::mutex m_cv_;
  std::mutex m_;
  std::map<std::string, int> prepared_;
  std::map<int, Value> store_;
  std::map<int, Callback> callbacks_;
  friend class FutureImpl;
  Logger::Ptr logger_;
};

class Exception : public std::exception {
 public:
  Exception(const std::string& m) : m_(m) {}
  const char* what() const noexcept override { return m_.c_str(); }

 private:
  std::string m_;
};

struct FutureImpl : public AbstractFuture {
  ResultSet Get(double timeout = 0) override;
  Value Get_(double timeout = 0);
  FutureImpl(int t, Connection::Ptr c) : ticket(t), conn(c) {}
  int ticket;
  Connection::Ptr conn;
};

inline Connection::Connection(const std::string& addr, int port,
                              const std::string& dbname,
                              const std::string& username,
                              const std::string& password, int timeout)
    : ip_(addr),
      port_(port),
      default_use_(dbname),
      username_(username),
      password_(password),
      default_timeout_(timeout),
      worker_(io_service_),
      socket_(io_service_),
      thread_([this]() { io_service_.run(); }),
      logger_(new Logger) {
  auto toks = Split(addr, "/");
  if (dbname.empty() && toks.size() > 1) {
    default_use_ = toks[1];
  }
  toks = Split(toks[0], "@");
  if (toks.size() > 1) {
    ip_ = toks[1];
    toks = Split(toks[0], ":");
    if (password.empty() && toks.size() > 1) password_ = toks[1];
    if (username.empty()) username_ = toks[0];
  } else {
    ip_ = toks[0];
  }
  toks = Split(ip_, ":");
  ip_ = toks[0];
  if (port <= 0 && toks.size() > 1) port_ = atoi(toks[1].c_str());
  if (port_ <= 0) port_ = 1116;
}

inline std::string Connection::Start() noexcept {
  if (connected_) return {};
  connected_ = -1;
  logger_->Info("OpenTick: Connecting");
  boost::asio::ip::tcp::endpoint end_pt(
      boost::asio::ip::address::from_string(ip_), port_);
  try {
    if (default_timeout_ <= 0) {
      socket_.connect(end_pt);
    } else {
      auto conn_result = socket_.async_connect(end_pt, boost::asio::use_future);
      // below future not work if Connection::Connect called in io_service
      // thread, dead loop
      auto status = conn_result.wait_for(seconds(default_timeout_));
      if (status == std::future_status::timeout) {
        throw Exception("connect timeout");
      }
      conn_result.get();
    }
    AfterConnected(true);
  } catch (std::exception& e) {
    Close();
    logger_->Error("OpenTick: Failed to connect: " + std::string(e.what()));
    return e.what();
  }
  return {};
}

inline void Connection::AfterConnected(bool sync) {
  boost::asio::ip::tcp::no_delay option(true);
  socket_.set_option(option);
  connected_ = 1;
  ReadHead();
  if (username_.size())
    Login(username_, password_, default_use_, sync);
  else if (default_use_.size())
    Use(default_use_, sync);
  logger_->Info("OpenTick: Connected");
}

inline void Connection::Close() {
  if (!connected_) return;
  connected_ = 0;
  auto self = shared_from_this();
  io_service_.post([self]() {
    boost::system::error_code ignoredCode;
    try {
      self->socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both,
                             ignoredCode);
      self->socket_.close();
    } catch (...) {
    }
    self->outbox_.clear();
    {
      std::lock_guard<std::mutex> lock(self->m_);
      self->prepared_.clear();
      self->callbacks_.clear();
    }
    {
      std::lock_guard<std::mutex> lk(self->m_cv_);
      self->store_.clear();
    }
    if (self->auto_reconnect_ > 0) {
      auto tt = new boost::asio::deadline_timer(
          self->io_service_, boost::posix_time::seconds(self->auto_reconnect_));
      tt->async_wait([self, tt](const boost::system::error_code&) {
        delete tt;
        boost::asio::ip::tcp::endpoint end_pt(
            boost::asio::ip::address::from_string(self->ip_), self->port_);
        self->logger_->Info("OpenTick: trying reconnect");
        self->socket_.async_connect(
            end_pt, [self](const boost::system::error_code& e) {
              if (e) {
                self->Close();
                self->logger_->Error("OpenTick: Failed to connect: " +
                                     std::string(e.message()));
                return;
              }
              self->AfterConnected(false);
            });
      });
    }
  });
}

inline void Connection::Login(const std::string& username,
                              const std::string& password,
                              const std::string& dbName, bool wait) {
  // username_/password_/default_use_ not thread safe
  username_ = username;
  password_ = password;
  auto arg = username_ + " " + password_;
  if (dbName.size()) {
    default_use_ = dbName;
    arg += " " + dbName;
  }
  auto ticket = ++ticket_counter_;
  Send(json::to_bson(json{{"0", ticket}, {"1", "login"}, {"2", arg}}));
  if (wait) FutureImpl(ticket, shared_from_this()).Get(default_timeout_);
}

inline void Connection::Use(const std::string& dbName, bool wait) {
  // default_use_ not thread safe
  default_use_ = dbName;
  auto ticket = ++ticket_counter_;
  Send(json::to_bson(json{{"0", ticket}, {"1", "use"}, {"2", dbName}}));
  if (wait) FutureImpl(ticket, shared_from_this()).Get(default_timeout_);
}

inline void Connection::ReadHead() {
  if (msg_in_buf_.size() < 4) msg_in_buf_.resize(4);
  auto self = shared_from_this();
  boost::asio::async_read(
      socket_, boost::asio::buffer(msg_in_buf_, 4),
      [=](const boost::system::error_code& e, size_t) {
        if (e) {
          self->Close();
          self->logger_->Error("OpenTick: Connection closed: " + e.message());
          self->Notify(-1, e.message());
          return;
        }
        unsigned n;
        memcpy(&n, msg_in_buf_.data(), 4);
        n = boost::endian::little_to_native(n);
        if (n)
          self->ReadBody(n);
        else
          self->ReadHead();
      });
}

inline void Connection::ReadBody(unsigned len) {
  msg_in_buf_.resize(len);
  auto self = shared_from_this();
  boost::asio::async_read(
      socket_, boost::asio::buffer(msg_in_buf_, len),
      [self, len](const boost::system::error_code& e, size_t) {
        if (e) {
          self->Close();
          self->logger_->Error("OpenTick: Connection closed: " + e.message());
          self->Notify(-1, e.message());
          return;
        }
        if (len == 1 && self->msg_in_buf_[0] == 'H') {
          self->Send(std::string(""));
          self->ReadHead();
          return;
        }
        try {
          auto j = json::from_bson(self->msg_in_buf_);
          auto ticket = j["0"].get<std::int64_t>();
          auto tmp = j["1"];
          if (tmp.is_string()) {
            self->Notify(ticket, tmp.get<std::string>());
          } else if (tmp.is_number_integer()) {
            self->Notify(ticket, tmp.get<std::int64_t>());
          } else if (tmp.is_number_float()) {
            self->Notify(ticket, tmp.get<double>());
          } else if (tmp.is_boolean()) {
            self->Notify(ticket, tmp.get<bool>());
          } else if (tmp.is_null()) {
            self->Notify(ticket, ValueScalar(nullptr));
          } else {
            auto v = std::make_shared<ValuesVector>();
            v->resize(tmp.size());
            for (auto i = 0u; i < tmp.size(); ++i) {
              auto& tmp2 = tmp[i];
              auto& v2 = (*v)[i];
              v2.resize(tmp2.size());
              for (auto j = 0u; j < tmp2.size(); ++j) {
                auto& v3 = v2[j];
                auto tmp3 = tmp2[j];
                if (tmp3.is_string()) {
                  v3 = tmp3.get<std::string>();
                } else if (tmp3.is_number_integer()) {
                  v3 = tmp3.get<std::int64_t>();
                } else if (tmp3.is_number_float()) {
                  v3 = tmp3.get<double>();
                } else if (tmp3.is_boolean()) {
                  v3 = tmp3.get<bool>();
                } else if (tmp3.is_array() && tmp3.size() == 2) {
                  auto sec = tmp3[0].get<std::int64_t>();
                  auto nsec = tmp3[1].get<std::int64_t>();
                  v3 = system_clock::from_time_t(sec) + nanoseconds(nsec);
                } else {
                  v3 = nullptr;
                }
              }
            }
            self->Notify(ticket, v);
          }
        } catch (nlohmann::detail::parse_error& e) {
          self->logger_->Error("OpenTick: Invalid bson");
        } catch (nlohmann::detail::exception& e) {
          self->logger_->Error("OpenTick: bson error: " +
                               std::string(e.what()));
        }
        self->ReadHead();
      });
}

template <typename T>
inline void Connection::Send(T&& msg) {
  if (!IsConnected()) return;
  auto self = shared_from_this();
  io_service_.post([msg = std::move(msg), self]() {
    auto& buf = self->msg_out_buf_;
    auto n0 = buf.size();
    buf.resize(n0 + 4 + msg.size());
    unsigned n = boost::endian::native_to_little(msg.size());
    memcpy(reinterpret_cast<void*>(buf.data() + n0), &n, 4);
    memcpy(reinterpret_cast<void*>(buf.data() + n0 + 4), msg.data(),
           msg.size());
    if (self->outbox_.size()) return;
    self->Write();
  });
}

inline void Connection::Write() {
  assert(outbox_.empty());
  outbox_.swap(msg_out_buf_);
  auto self = shared_from_this();
  boost::asio::async_write(
      socket_, boost::asio::buffer(outbox_, outbox_.size()),
      [self](const boost::system::error_code& e, std::size_t) {
        if (e) {
          self->Close();
          self->logger_->Error(
              "OpenTick: Failed to send message. Error code: " + e.message());
          self->Notify(-1, e.message());
        } else {
          self->outbox_.clear();
          if (self->msg_out_buf_.size()) self->Write();
        }
      });
}

inline int Connection::Prepare(const std::string& sql) {
  {
    std::lock_guard<std::mutex> lock(m_);
    auto it = prepared_.find(sql);
    if (it != prepared_.end()) return it->second;
  }
  auto ticket = ++ticket_counter_;
  Send(json::to_bson(json{{"0", ticket}, {"1", "prepare"}, {"2", sql}}));
  FutureImpl f(ticket, shared_from_this());
  auto id = std::get<std::int64_t>(std::get<ValueScalar>(f.Get_()));
  {
    std::lock_guard<std::mutex> lock(m_);
    prepared_.emplace(sql, id);
  }
  return id;
}

inline Value FutureImpl::Get_(double timeout) {
  std::unique_lock<std::mutex> lk(conn->m_cv_);
  auto start = system_clock::now();
  timeout *= 1e6;
  while (true) {
    auto it1 = conn->store_.find(ticket);
    if (it1 != conn->store_.end()) {
      auto tmp = it1->second;
      conn->store_.erase(it1);
      if (auto ptr = std::get_if<ValueScalar>(&tmp)) {
        if (auto ptr2 = std::get_if<std::string>(ptr)) throw Exception(*ptr2);
      }
      return tmp;
    }
    auto it2 = conn->store_.find(-1);
    if (it2 != conn->store_.end()) {
      throw Exception(
          std::get<std::string>(std::get<ValueScalar>(it2->second)));
    }
    using namespace std::chrono_literals;
    conn->cv_.wait_for(lk, 1ms);
    if (timeout > 0 &&
        duration_cast<microseconds>(system_clock::now() - start).count() >=
            timeout) {
      throw Exception("Timeout");
    }
  }
  return {};
}

inline ResultSet FutureImpl::Get(double timeout) {
  auto v = Get_(timeout);
  if (auto ptr = std::get_if<ResultSet>(&v)) return *ptr;
  return {};
}

inline void Connection::Notify(int ticket, const Value& value) {
  Callback callback;
  {
    std::lock_guard<std::mutex> lock(m_);
    auto it = callbacks_.find(ticket);
    if (it != callbacks_.end()) {
      callback = it->second;
      callbacks_.erase(it);
      if (!callback) return;  // timeout
    }
  }
  if (callback) {
    if (auto ptr = std::get_if<ValueScalar>(&value)) {
      if (auto ptr2 = std::get_if<std::string>(ptr)) {
        callback({}, *ptr2);
      }
    } else if (auto ptr = std::get_if<ResultSet>(&value)) {
      callback(*ptr, "");
    }
    return;
  }
  std::lock_guard<std::mutex> lk(m_cv_);
  store_[ticket] = value;
  cv_.notify_all();
}

inline void ConvertArgs(const Args& args, json& jargs) {
  for (auto& v : args) {
    std::visit(
        [&jargs](auto&& v2) {
          using T = std::decay_t<decltype(v2)>;
          if constexpr (std::is_same_v<T, Tm>) {
            auto d = duration_cast<nanoseconds>(v2.time_since_epoch()).count();
            jargs.push_back(json{d / 1000000000, d % 1000000000});
          } else {
            jargs.push_back(v2);
          }
        },
        v);
  }
}

inline Future Connection::ExecuteAsync(const std::string& sql, const Args& args,
                                       Callback callback) {
  auto prepared = -1;
  json jargs;
  if (args.size()) {
    ConvertArgs(args, jargs);
    prepared = Prepare(sql);
  }
  auto ticket = ++ticket_counter_;
  json j = {{"0", ticket}, {"1", "run"}, {"2", sql}, {"3", jargs}};
  if (prepared >= 0) j["2"] = prepared;
  Send(json::to_bson(j));
  if (callback) {
    {
      std::lock_guard<std::mutex> lock(m_);
      callbacks_[ticket] = callback;
    }
    if (default_timeout_ > 0) {
      auto tt = new boost::asio::deadline_timer(
          io_service_, boost::posix_time::seconds(default_timeout_));
      auto self = shared_from_this();
      tt->async_wait([self, ticket, tt](const boost::system::error_code&) {
        delete tt;
        Callback callback;
        {
          std::lock_guard<std::mutex> lock(self->m_);
          auto it = self->callbacks_.find(ticket);
          if (it == self->callbacks_.end()) return;
          callback = it->second;
          // reset it rather than erase to let Notify handle it for memory leak
          // issue of store_
          it->second = {};
        }
        if (callback) callback({}, "timeout");
      });
    }
    return {};
  }
  return Future(new FutureImpl(ticket, shared_from_this()));
}

inline ResultSet Connection::Execute(const std::string& sql, const Args& args) {
  return ExecuteAsync(sql, args)->Get(default_timeout_);
}

inline Future Connection::BatchInsertAsync(const std::string& sql,
                                           const Argss& argss) {
  std::vector<json> data;
  data.resize(argss.size());
  auto i = 0u;
  for (auto& args : argss) {
    ConvertArgs(args, data[i++]);
  }
  auto prepared = Prepare(sql);
  auto ticket = ++ticket_counter_;
  Send(json::to_bson(
      json{{"0", ticket}, {"1", "batch"}, {"2", prepared}, {"3", data}}));
  return Future(new FutureImpl(ticket, shared_from_this()));
}

inline void Connection::BatchInsert(const std::string& sql,
                                    const Argss& argss) {
  BatchInsertAsync(sql, argss)->Get(default_timeout_);
}

}  // namespace opentick

#endif  // OPENTICK_CONNECTION_H_
