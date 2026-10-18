#ifndef BANK_OS_JSON_HPP
#define BANK_OS_JSON_HPP

#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <iomanip>
#include <stdexcept>
#include <cctype>
#include <initializer_list>
#include <memory>
#include <iostream>

namespace nlohmann {

class json {
public:
    enum class Type { Null, Boolean, Number, String, Array, Object };

private:
    Type type_ = Type::Null;
    bool bool_val_ = false;
    double num_val_ = 0.0;
    std::string str_val_;
    std::vector<json> arr_val_;
    std::map<std::string, json> obj_val_;

public:
    json() : type_(Type::Null) {}
    json(std::nullptr_t) : type_(Type::Null) {}
    json(bool b) : type_(Type::Boolean), bool_val_(b) {}
    json(int v) : type_(Type::Number), num_val_(static_cast<double>(v)) {}
    json(long v) : type_(Type::Number), num_val_(static_cast<double>(v)) {}
    json(long long v) : type_(Type::Number), num_val_(static_cast<double>(v)) {}
    json(unsigned int v) : type_(Type::Number), num_val_(static_cast<double>(v)) {}
    json(unsigned long v) : type_(Type::Number), num_val_(static_cast<double>(v)) {}
    json(unsigned long long v) : type_(Type::Number), num_val_(static_cast<double>(v)) {}
    json(double v) : type_(Type::Number), num_val_(v) {}
    json(float v) : type_(Type::Number), num_val_(static_cast<double>(v)) {}
    json(const char* s) : type_(Type::String), str_val_(s ? s : "") {}
    json(const std::string& s) : type_(Type::String), str_val_(s) {}
    json(const std::vector<json>& arr) : type_(Type::Array), arr_val_(arr) {}
    json(const std::map<std::string, json>& obj) : type_(Type::Object), obj_val_(obj) {}

    // Initializer list support
    json(std::initializer_list<std::pair<const std::string, json>> init) : type_(Type::Object) {
        for (const auto& item : init) {
            obj_val_[item.first] = item.second;
        }
    }

    static json array() {
        json j;
        j.type_ = Type::Array;
        return j;
    }

    static json object() {
        json j;
        j.type_ = Type::Object;
        return j;
    }

    bool is_null() const { return type_ == Type::Null; }
    bool is_boolean() const { return type_ == Type::Boolean; }
    bool is_number() const { return type_ == Type::Number; }
    bool is_string() const { return type_ == Type::String; }
    bool is_array() const { return type_ == Type::Array; }
    bool is_object() const { return type_ == Type::Object; }

    Type type() const { return type_; }

    void push_back(const json& val) {
        if (type_ == Type::Null) {
            type_ = Type::Array;
        }
        if (type_ != Type::Array) {
            throw std::runtime_error("push_back called on non-array json");
        }
        arr_val_.push_back(val);
    }

    json& operator[](const std::string& key) {
        if (type_ == Type::Null) {
            type_ = Type::Object;
        }
        if (type_ != Type::Object) {
            throw std::runtime_error("operator[key] called on non-object json");
        }
        return obj_val_[key];
    }

    const json& operator[](const std::string& key) const {
        if (type_ != Type::Object) {
            static const json null_json;
            return null_json;
        }
        auto it = obj_val_.find(key);
        if (it != obj_val_.end()) {
            return it->second;
        }
        static const json null_json;
        return null_json;
    }

    json& operator[](size_t index) {
        if (type_ != Type::Array) {
            throw std::runtime_error("operator[index] called on non-array json");
        }
        if (index >= arr_val_.size()) {
            throw std::out_of_range("json array index out of range");
        }
        return arr_val_[index];
    }

    const json& operator[](size_t index) const {
        if (type_ != Type::Array) {
            static const json null_json;
            return null_json;
        }
        if (index >= arr_val_.size()) {
            throw std::out_of_range("json array index out of range");
        }
        return arr_val_[index];
    }

    size_t size() const {
        if (type_ == Type::Array) return arr_val_.size();
        if (type_ == Type::Object) return obj_val_.size();
        return 0;
    }

    bool empty() const {
        return size() == 0;
    }

    bool contains(const std::string& key) const {
        if (type_ != Type::Object) return false;
        return obj_val_.find(key) != obj_val_.end();
    }

    const std::string& get_string_ref() const {
        if (type_ == Type::String) return str_val_;
        throw std::runtime_error("json value is not a string");
    }

    template<typename T>
    T get() const;

    std::string dump(int indent = -1) const {
        std::ostringstream ss;
        serialize(ss, indent, 0);
        return ss.str();
    }

    static json parse(const std::string& str) {
        size_t idx = 0;
        skip_ws(str, idx);
        return parse_value(str, idx);
    }

private:
    static void skip_ws(const std::string& s, size_t& idx) {
        while (idx < s.size() && (s[idx] == ' ' || s[idx] == '\t' || s[idx] == '\r' || s[idx] == '\n')) {
            idx++;
        }
    }

    static json parse_value(const std::string& s, size_t& idx) {
        skip_ws(s, idx);
        if (idx >= s.size()) return json();

        char c = s[idx];
        if (c == 'n') {
            if (s.substr(idx, 4) == "null") { idx += 4; return json(); }
        } else if (c == 't') {
            if (s.substr(idx, 4) == "true") { idx += 4; return json(true); }
        } else if (c == 'f') {
            if (s.substr(idx, 5) == "false") { idx += 5; return json(false); }
        } else if (c == '"') {
            return parse_string(s, idx);
        } else if (c == '[') {
            return parse_array(s, idx);
        } else if (c == '{') {
            return parse_object(s, idx);
        } else if (c == '-' || (c >= '0' && c <= '9')) {
            return parse_number(s, idx);
        }
        throw std::runtime_error(std::string("Unexpected character in json: ") + c);
    }

    static json parse_string(const std::string& s, size_t& idx) {
        idx++; // skip "
        std::string res;
        while (idx < s.size()) {
            char c = s[idx++];
            if (c == '"') return json(res);
            if (c == '\\' && idx < s.size()) {
                char esc = s[idx++];
                if (esc == '"') res += '"';
                else if (esc == '\\') res += '\\';
                else if (esc == '/') res += '/';
                else if (esc == 'b') res += '\b';
                else if (esc == 'f') res += '\f';
                else if (esc == 'n') res += '\n';
                else if (esc == 'r') res += '\r';
                else if (esc == 't') res += '\t';
                else res += esc;
            } else {
                res += c;
            }
        }
        throw std::runtime_error("Unterminated string in json");
    }

    static json parse_number(const std::string& s, size_t& idx) {
        size_t start = idx;
        if (s[idx] == '-') idx++;
        while (idx < s.size() && std::isdigit(static_cast<unsigned char>(s[idx]))) idx++;
        if (idx < s.size() && s[idx] == '.') {
            idx++;
            while (idx < s.size() && std::isdigit(static_cast<unsigned char>(s[idx]))) idx++;
        }
        if (idx < s.size() && (s[idx] == 'e' || s[idx] == 'E')) {
            idx++;
            if (idx < s.size() && (s[idx] == '+' || s[idx] == '-')) idx++;
            while (idx < s.size() && std::isdigit(static_cast<unsigned char>(s[idx]))) idx++;
        }
        double val = std::stod(s.substr(start, idx - start));
        return json(val);
    }

    static json parse_array(const std::string& s, size_t& idx) {
        idx++; // skip [
        json res = json::array();
        skip_ws(s, idx);
        if (idx < s.size() && s[idx] == ']') {
            idx++;
            return res;
        }
        while (idx < s.size()) {
            res.push_back(parse_value(s, idx));
            skip_ws(s, idx);
            if (idx < s.size() && s[idx] == ',') {
                idx++;
                skip_ws(s, idx);
            } else if (idx < s.size() && s[idx] == ']') {
                idx++;
                return res;
            } else {
                break;
            }
        }
        throw std::runtime_error("Unterminated array in json");
    }

    static json parse_object(const std::string& s, size_t& idx) {
        idx++; // skip {
        json res = json::object();
        skip_ws(s, idx);
        if (idx < s.size() && s[idx] == '}') {
            idx++;
            return res;
        }
        while (idx < s.size()) {
            skip_ws(s, idx);
            if (idx >= s.size() || s[idx] != '"') {
                throw std::runtime_error("Expected string key in object");
            }
            json key_json = parse_string(s, idx);
            std::string key = key_json.get_string_ref();
            skip_ws(s, idx);
            if (idx >= s.size() || s[idx] != ':') {
                throw std::runtime_error("Expected ':' after key in object");
            }
            idx++; // skip :
            json val = parse_value(s, idx);
            res[key] = val;
            skip_ws(s, idx);
            if (idx < s.size() && s[idx] == ',') {
                idx++;
                skip_ws(s, idx);
            } else if (idx < s.size() && s[idx] == '}') {
                idx++;
                return res;
            } else {
                break;
            }
        }
        throw std::runtime_error("Unterminated object in json");
    }

    void serialize(std::ostream& os, int indent, int current_indent) const {
        std::string ind_str = (indent >= 0) ? std::string(current_indent, ' ') : "";
        std::string next_ind_str = (indent >= 0) ? std::string(current_indent + indent, ' ') : "";
        std::string nl = (indent >= 0) ? "\n" : "";
        std::string sp = (indent >= 0) ? " " : "";

        switch (type_) {
            case Type::Null:
                os << "null";
                break;
            case Type::Boolean:
                os << (bool_val_ ? "true" : "false");
                break;
            case Type::Number: {
                if (num_val_ == static_cast<long long>(num_val_)) {
                    os << static_cast<long long>(num_val_);
                } else {
                    os << std::setprecision(10) << num_val_;
                }
                break;
            }
            case Type::String: {
                os << '"';
                for (char c : str_val_) {
                    if (c == '"') os << "\\\"";
                    else if (c == '\\') os << "\\\\";
                    else if (c == '\b') os << "\\b";
                    else if (c == '\f') os << "\\f";
                    else if (c == '\n') os << "\\n";
                    else if (c == '\r') os << "\\r";
                    else if (c == '\t') os << "\\t";
                    else os << c;
                }
                os << '"';
                break;
            }
            case Type::Array: {
                if (arr_val_.empty()) {
                    os << "[]";
                } else {
                    os << "[" << nl;
                    for (size_t i = 0; i < arr_val_.size(); ++i) {
                        os << next_ind_str;
                        arr_val_[i].serialize(os, indent, current_indent + (indent >= 0 ? indent : 0));
                        if (i + 1 < arr_val_.size()) os << ",";
                        os << nl;
                    }
                    os << ind_str << "]";
                }
                break;
            }
            case Type::Object: {
                if (obj_val_.empty()) {
                    os << "{}";
                } else {
                    os << "{" << nl;
                    size_t i = 0;
                    for (const auto& kv : obj_val_) {
                        os << next_ind_str << '"' << kv.first << "\":" << sp;
                        kv.second.serialize(os, indent, current_indent + (indent >= 0 ? indent : 0));
                        if (++i < obj_val_.size()) os << ",";
                        os << nl;
                    }
                    os << ind_str << "}";
                }
                break;
            }
        }
    }
};

template<>
inline int json::get<int>() const {
    if (type_ == Type::Number) return static_cast<int>(num_val_);
    if (type_ == Type::Boolean) return bool_val_ ? 1 : 0;
    throw std::runtime_error("json value is not a number");
}

template<>
inline long long json::get<long long>() const {
    if (type_ == Type::Number) return static_cast<long long>(num_val_);
    throw std::runtime_error("json value is not a number");
}

template<>
inline double json::get<double>() const {
    if (type_ == Type::Number) return num_val_;
    throw std::runtime_error("json value is not a number");
}

template<>
inline float json::get<float>() const {
    if (type_ == Type::Number) return static_cast<float>(num_val_);
    throw std::runtime_error("json value is not a number");
}

template<>
inline bool json::get<bool>() const {
    if (type_ == Type::Boolean) return bool_val_;
    if (type_ == Type::Number) return num_val_ != 0.0;
    throw std::runtime_error("json value is not a boolean");
}

template<>
inline std::string json::get<std::string>() const {
    if (type_ == Type::String) return str_val_;
    throw std::runtime_error("json value is not a string");
}

} // namespace nlohmann

// Convenient alias
using json = nlohmann::json;

#endif // BANK_OS_JSON_HPP
