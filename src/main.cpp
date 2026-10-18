#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <csignal>
#include "bank_os.hpp"
#include "httplib.h"
#include "json.hpp"

static std::atomic<bool> g_running{true};

void sig_handler(int sig) {
    (void)sig;
    std::cout << "\nShutting down Adaptive Bank OS...\n";
    g_running = false;
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, sig_handler);
    std::signal(SIGTERM, sig_handler);

    int port = 8080;
    size_t workload_size = 176000;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            port = std::atoi(argv[++i]);
        } else if (arg == "--workload" && i + 1 < argc) {
            workload_size = std::stoul(argv[++i]);
        }
    }

    std::cout << "========================================================\n";
    std::cout << "  ADAPTIVE BANK OS — AUTONOMOUS MISSION CONTROL SERVER  \n";
    std::cout << "  Workload Size: " << workload_size << " transactions\n";
    std::cout << "  Port: " << port << " | Static Web: ./web\n";
    std::cout << "========================================================\n";

    // Initialize Simulator with 176k transactions
    auto sim = std::make_shared<BankOSSimulator>(workload_size, true, 50);

    // Simulation background thread
    std::thread sim_worker([sim]() {
        while (g_running) {
            if (!sim->is_paused()) {
                sim->step_tick();
            }
            int delay_us = sim->get_tick_delay_us();
            if (delay_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
            } else {
                std::this_thread::yield();
            }
        }
    });

    // Configure HTTP Web Server
    httplib::Server svr;
    svr.set_mount_point("/", "./web");

    // 1. GET /api/status
    svr.Get("/api/status", [sim](const httplib::Request&, httplib::Response& res) {
        res.set_content(sim->get_status_json().dump(), "application/json");
    });

    // 2. GET /api/state
    svr.Get("/api/state", [sim](const httplib::Request&, httplib::Response& res) {
        res.set_content(sim->get_state_json().dump(), "application/json");
    });

    // 3. GET /api/queues
    svr.Get("/api/queues", [sim](const httplib::Request&, httplib::Response& res) {
        res.set_content(sim->get_queues_json().dump(), "application/json");
    });

    // 4. GET /api/transitions
    svr.Get("/api/transitions", [sim](const httplib::Request& req, httplib::Response& res) {
        size_t limit = 100;
        if (req.has_param("limit")) {
            try { limit = std::stoul(req.get_param_value("limit")); } catch (...) {}
        }
        res.set_content(sim->get_transitions_json(limit).dump(), "application/json");
    });

    // 5. POST /api/comparison-mode
    svr.Post("/api/comparison-mode", [sim](const httplib::Request& req, httplib::Response& res) {
        bool adaptive = true;
        try {
            if (!req.body.empty()) {
                json body = json::parse(req.body);
                if (body.contains("adaptive")) {
                    adaptive = body["adaptive"].get<bool>();
                }
            }
        } catch (...) {}
        sim->set_adaptive_mode(adaptive);
        json out = json::object();
        out["status"] = "ok";
        out["comparison_mode"] = adaptive ? "ADAPTIVE_MDACC" : "STANDARD_BASELINE";
        res.set_content(out.dump(), "application/json");
    });

    // 6. GET /api/control/pause & /api/control/resume
    svr.Get("/api/control/pause", [sim](const httplib::Request&, httplib::Response& res) {
        sim->set_paused(true);
        res.set_content("{\"status\":\"ok\",\"paused\":true}", "application/json");
    });

    svr.Get("/api/control/resume", [sim](const httplib::Request&, httplib::Response& res) {
        sim->set_paused(false);
        res.set_content("{\"status\":\"ok\",\"paused\":false}", "application/json");
    });

    // 7. GET /api/control/speed?delay=<microseconds>
    svr.Get("/api/control/speed", [sim](const httplib::Request& req, httplib::Response& res) {
        int delay = 1000;
        if (req.has_param("delay")) {
            try { delay = std::stoi(req.get_param_value("delay")); } catch (...) {}
        }
        sim->set_tick_delay_us(delay);
        json out = json::object();
        out["status"] = "ok";
        out["tick_delay_us"] = delay;
        res.set_content(out.dump(), "application/json");
    });

    // 8. GET /api/export
    svr.Get("/api/export", [sim](const httplib::Request&, httplib::Response& res) {
        std::string filename = "state_transitions.json";
        sim->export_transitions(filename, 5000);
        
        // Also return the content in response with download header
        std::ifstream ifs(filename);
        if (ifs.is_open()) {
            std::stringstream ss;
            ss << ifs.rdbuf();
            res.set_header("Content-Disposition", "attachment; filename=\"state_transitions.json\"");
            res.set_content(ss.str(), "application/json");
        } else {
            res.set_content("{\"error\":\"Failed to export\"}", "application/json");
        }
    });

    std::cout << "Starting server on port " << port << "...\n";
    svr.listen("0.0.0.0", port);

    g_running = false;
    if (sim_worker.joinable()) {
        sim_worker.join();
    }

    return 0;
}
