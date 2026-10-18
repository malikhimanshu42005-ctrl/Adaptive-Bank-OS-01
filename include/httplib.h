#ifndef BANK_OS_HTTPLIB_H
#define BANK_OS_HTTPLIB_H

#include <string>
#include <vector>
#include <map>
#include <functional>
#include <thread>
#include <mutex>
#include <atomic>
#include <sstream>
#include <fstream>
#include <iostream>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

namespace httplib {

struct Request {
    std::string method;
    std::string path;
    std::string body;
    std::map<std::string, std::string> headers;
    std::map<std::string, std::string> params;

    bool has_param(const std::string& key) const {
        return params.find(key) != params.end();
    }

    std::string get_param_value(const std::string& key, size_t id = 0) const {
        (void)id;
        auto it = params.find(key);
        if (it != params.end()) {
            return it->second;
        }
        return "";
    }
};

struct Response {
    int status = 200;
    std::map<std::string, std::string> headers;
    std::string body;

    void set_content(const std::string& s, const std::string& content_type) {
        body = s;
        headers["Content-Type"] = content_type;
    }

    void set_header(const std::string& key, const std::string& val) {
        headers[key] = val;
    }
};

class Server {
public:
    using Handler = std::function<void(const Request&, Response&)>;

private:
    std::map<std::string, Handler> get_handlers_;
    std::map<std::string, Handler> post_handlers_;
    std::string mount_prefix_;
    std::string mount_dir_;
    std::atomic<bool> is_running_{false};
    int server_sock_ = -1;
    std::mutex handlers_mtx_;

public:
    Server() = default;
    ~Server() {
        stop();
    }

    void set_mount_point(const std::string& mount_prefix, const std::string& dir) {
        mount_prefix_ = mount_prefix;
        mount_dir_ = dir;
        while (!mount_dir_.empty() && (mount_dir_.back() == '/' || mount_dir_.back() == '\\')) {
            mount_dir_.pop_back();
        }
    }

    Server& Get(const std::string& pattern, Handler handler) {
        std::lock_guard<std::mutex> lock(handlers_mtx_);
        get_handlers_[pattern] = handler;
        return *this;
    }

    Server& Post(const std::string& pattern, Handler handler) {
        std::lock_guard<std::mutex> lock(handlers_mtx_);
        post_handlers_[pattern] = handler;
        return *this;
    }

    bool is_valid() const {
        return true;
    }

    void stop() {
        if (is_running_.exchange(false)) {
            if (server_sock_ >= 0) {
                shutdown(server_sock_, SHUT_RDWR);
                close(server_sock_);
                server_sock_ = -1;
            }
        }
    }

    bool listen(const std::string& host, int port) {
        server_sock_ = socket(AF_INET, SOCK_STREAM, 0);
        if (server_sock_ < 0) {
            std::cerr << "Failed to create socket\n";
            return false;
        }

        int opt = 1;
        setsockopt(server_sock_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (host == "0.0.0.0" || host.empty()) {
            addr.sin_addr.s_addr = INADDR_ANY;
        } else {
            inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
        }

        if (bind(server_sock_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "Failed to bind to " << host << ":" << port << "\n";
            close(server_sock_);
            server_sock_ = -1;
            return false;
        }

        if (::listen(server_sock_, 128) < 0) {
            std::cerr << "Failed to listen on socket\n";
            close(server_sock_);
            server_sock_ = -1;
            return false;
        }

        is_running_ = true;
        std::cout << "Bank OS Web Server listening on http://" << host << ":" << port << "\n";

        while (is_running_) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            int client_sock = accept(server_sock_, (struct sockaddr*)&client_addr, &client_len);
            if (client_sock < 0) {
                if (!is_running_) break;
                continue;
            }

            std::thread([this, client_sock]() {
                handle_client(client_sock);
            }).detach();
        }

        return true;
    }

private:
    static std::string get_mime_type(const std::string& path) {
        if (path.rfind(".html") != std::string::npos) return "text/html; charset=utf-8";
        if (path.rfind(".css") != std::string::npos) return "text/css; charset=utf-8";
        if (path.rfind(".js") != std::string::npos) return "application/javascript; charset=utf-8";
        if (path.rfind(".json") != std::string::npos) return "application/json; charset=utf-8";
        if (path.rfind(".svg") != std::string::npos) return "image/svg+xml";
        if (path.rfind(".png") != std::string::npos) return "image/png";
        if (path.rfind(".ico") != std::string::npos) return "image/x-icon";
        return "text/plain; charset=utf-8";
    }

    void handle_client(int sock) {
        std::vector<char> buffer(8192);
        std::string raw_request;
        ssize_t bytes_read = 0;

        while ((bytes_read = recv(sock, buffer.data(), buffer.size(), 0)) > 0) {
            raw_request.append(buffer.data(), bytes_read);
            if (raw_request.find("\r\n\r\n") != std::string::npos) {
                // Check content length if any
                size_t cl_pos = raw_request.find("Content-Length:");
                if (cl_pos == std::string::npos) cl_pos = raw_request.find("content-length:");
                if (cl_pos != std::string::npos) {
                    size_t cl_end = raw_request.find("\r\n", cl_pos);
                    std::string cl_str = raw_request.substr(cl_pos + 15, cl_end - (cl_pos + 15));
                    try {
                        size_t expected_body_len = std::stoul(cl_str);
                        size_t header_end = raw_request.find("\r\n\r\n") + 4;
                        size_t current_body_len = raw_request.size() - header_end;
                        while (current_body_len < expected_body_len) {
                            ssize_t extra = recv(sock, buffer.data(), buffer.size(), 0);
                            if (extra <= 0) break;
                            raw_request.append(buffer.data(), extra);
                            current_body_len += extra;
                        }
                    } catch (...) {}
                }
                break;
            }
        }

        if (raw_request.empty()) {
            close(sock);
            return;
        }

        Request req;
        Response res;
        parse_request(raw_request, req);

        // Preflight OPTIONS support for CORS
        if (req.method == "OPTIONS") {
            res.status = 204;
            send_response(sock, req, res);
            close(sock);
            return;
        }

        bool handled = false;
        Handler chosen_handler = nullptr;

        {
            std::lock_guard<std::mutex> lock(handlers_mtx_);
            if (req.method == "GET") {
                auto it = get_handlers_.find(req.path);
                if (it != get_handlers_.end()) {
                    chosen_handler = it->second;
                }
            } else if (req.method == "POST") {
                auto it = post_handlers_.find(req.path);
                if (it != post_handlers_.end()) {
                    chosen_handler = it->second;
                }
            }
        }

        if (chosen_handler) {
            try {
                chosen_handler(req, res);
                handled = true;
            } catch (const std::exception& e) {
                res.status = 500;
                res.set_content(std::string("{\"error\":\"") + e.what() + "\"}", "application/json");
                handled = true;
            }
        }

        // Static files if not handled by API
        if (!handled && req.method == "GET" && !mount_dir_.empty()) {
            std::string file_path = req.path;
            if (file_path == "/" || file_path.empty()) {
                file_path = "/index.html";
            }
            std::string full_path = mount_dir_ + file_path;
            std::ifstream ifs(full_path, std::ios::binary);
            if (ifs.is_open()) {
                std::stringstream ss;
                ss << ifs.rdbuf();
                res.status = 200;
                res.set_content(ss.str(), get_mime_type(full_path));
                handled = true;
            }
        }

        if (!handled) {
            res.status = 404;
            res.set_content("{\"error\":\"Not Found\"}", "application/json");
        }

        send_response(sock, req, res);
        close(sock);
    }

    void parse_request(const std::string& raw, Request& req) {
        std::istringstream stream(raw);
        std::string request_line;
        if (!std::getline(stream, request_line)) return;

        if (!request_line.empty() && request_line.back() == '\r') {
            request_line.pop_back();
        }

        std::istringstream line_stream(request_line);
        std::string full_path, version;
        line_stream >> req.method >> full_path >> version;

        // Parse path and query parameters
        size_t query_pos = full_path.find('?');
        if (query_pos != std::string::npos) {
            req.path = full_path.substr(0, query_pos);
            std::string query = full_path.substr(query_pos + 1);
            std::istringstream query_stream(query);
            std::string param_pair;
            while (std::getline(query_stream, param_pair, '&')) {
                size_t eq_pos = param_pair.find('=');
                if (eq_pos != std::string::npos) {
                    std::string k = param_pair.substr(0, eq_pos);
                    std::string v = param_pair.substr(eq_pos + 1);
                    req.params[k] = v;
                } else if (!param_pair.empty()) {
                    req.params[param_pair] = "";
                }
            }
        } else {
            req.path = full_path;
        }

        // Headers
        std::string header_line;
        while (std::getline(stream, header_line) && header_line != "\r" && !header_line.empty()) {
            if (header_line.back() == '\r') header_line.pop_back();
            size_t colon = header_line.find(':');
            if (colon != std::string::npos) {
                std::string k = header_line.substr(0, colon);
                std::string v = header_line.substr(colon + 1);
                while (!v.empty() && v.front() == ' ') v.erase(v.begin());
                req.headers[k] = v;
            }
        }

        // Body
        size_t body_pos = raw.find("\r\n\r\n");
        if (body_pos != std::string::npos) {
            req.body = raw.substr(body_pos + 4);
        }
    }

    void send_response(int sock, const Request& req, const Response& res) {
        (void)req;
        std::ostringstream ss;
        std::string status_msg = "OK";
        if (res.status == 204) status_msg = "No Content";
        else if (res.status == 400) status_msg = "Bad Request";
        else if (res.status == 404) status_msg = "Not Found";
        else if (res.status == 500) status_msg = "Internal Server Error";

        ss << "HTTP/1.1 " << res.status << " " << status_msg << "\r\n";
        ss << "Access-Control-Allow-Origin: *\r\n";
        ss << "Access-Control-Allow-Methods: GET, POST, OPTIONS, PUT, DELETE\r\n";
        ss << "Access-Control-Allow-Headers: Content-Type, Authorization\r\n";
        ss << "Connection: close\r\n";

        for (const auto& h : res.headers) {
            ss << h.first << ": " << h.second << "\r\n";
        }

        ss << "Content-Length: " << res.body.size() << "\r\n\r\n";
        ss << res.body;

        std::string out = ss.str();
        send(sock, out.data(), out.size(), 0);
    }
};

} // namespace httplib

#endif // BANK_OS_HTTPLIB_H
