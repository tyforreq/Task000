#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <iomanip>

const char *FTIME = "%Y/%m/%d %H:%M:%S";

// サーバーを表現するクラス
struct Server {
    std::uint32_t address;
    std::uint32_t subnet;
    int subnet_id;
    std::vector<std::tm> dates;
    std::vector<int> responses;
};


// 故障などの状態を管理
struct Event {
    int type; // 0 : down / 1: overload
    int server_id_in_subnet; // subnet内で何番目か記録
    std::tm start;
    std::tm end;
    std::string text;
};


// 入力データの読み込み
// 結果を第2引数へ格納 (key: addressのstring, value: Server)
extern void read_log(const std::string &filename, std::vector<Server> &servers);


// 課題1, 2, 4
// 読み込んだサーバーをすべて出力する (デバッグ用)
extern void print_all(const std::vector<Server> &servers);


// 課題1, 2, 3
// 故障or各負荷サーバーの出力
extern void print_failures(std::vector<Server> &servers, int N=0, float t=0.0, int m=0);

// 課題4
// ネットワークスイッチの故障検出
extern void print_network_failures(std::vector<Server> &servers, std::vector<std::vector<int>> &networks, int N);


// サーバーが故障しているか調べる
// 故障してる場合, そのログを第1引数へ格納
extern void calc_failure(std::vector<Event> &log, Server &server, int N);


// サーバーが過負荷になっているか調べる
// 該当ログを第1引数へ
extern void calc_overload(std::vector<Event> &log, Server &server,float t, int m);


// 同じsubnetに属するサーバーをまとめる
// 第2引数: networks[i][j] で i番目のsubnetのj番目のサーバー
extern void calc_networks(std::vector<Server> &servers, std::vector<std::vector<int>> &networks);


// 集約したnetwork情報を出力 (debug用)
extern void print_networks(std::vector<Server> &servers, std::vector<std::vector<int>> &networks);


// 32bitアドレスの上位mask bitを取り出す
extern std::uint32_t calc_subnet(std::uint32_t address, int mask);


// 32bitアドレスをWWW.XXX.YYY.ZZZに変換
extern std::uint32_t address_to_int(const std::string &address);


// WWW.XXX.YYY.ZZZ を32bitへ変換
extern std::string address_to_str(std::uint32_t address);


int main(int argc, char **argv) {

    std::string filename = "data/sample.csv";
    bool task_1 = false;
    bool task_2 = false;
    bool task_3 = false;
    bool task_4 = false;
    int N = 0;
    float t = 0;
    int m = 0;
    

    // コマンドライン引数の解析
    int i;
    for (i = 1; i < argc; ++i) {
        std::string argstr(argv[i]);
        if (argstr == "--input" || argstr == "-i") filename = argv[++i];
        else if (argstr == "--task1") task_1 = true;
        else if (argstr == "--task2") task_2 = true;
        else if (argstr == "--task3") task_3 = true;
        else if (argstr == "--task4") task_4 = true;
        else if (argstr == "-N") N = std::stoi(argv[++i]);
        else if (argstr == "-t") t = std::stof(argv[++i]);
        else if (argstr == "-m") m = std::stoi(argv[++i]);
        else {
            std::cerr << "Undefined keyword:" << argstr << " given." << std::endl;
            exit(1);
        }
    }


    // ファイル読み込み
    std::vector<Server> servers;
    read_log(filename, servers);
    
    // ネットワーク集計
    std::vector<std::vector<int>> networks;
    calc_networks(servers, networks);

    std::cout << "Parameters: N=" << N << ", t=" << t << ", m=" << m << std::endl; 


    #ifdef DEBUG
    print_all(servers);
    print_networks(servers, networks);
    #endif


    // 課題
    if (task_1) {
        std::cout << "-- Task 1. --" << std::endl;
        print_failures(servers);
        std::cout << std::endl;
    }
    
    if (task_2) {
        std::cout << "-- Task 2. --" << std::endl;
        print_failures(servers, N);
        std::cout << std::endl;
    }

    if (task_3) {
        std::cout << "-- Task 3. --" << std::endl;
        print_failures(servers, N, t, m);
        std::cout << std::endl;
    }

    if (task_4) {
        std::cout << "-- Task 4. --" << std::endl;
        print_network_failures(servers, networks, N);
        std::cout << std::endl;
    } 
}

// ログ・ファイルの読み込み
void read_log(const std::string &filename, std::vector<Server> &servers) {

    int id;
    std::ifstream ifs(filename);
    std::string line;
    std::string item;
    std::unordered_map<std::uint32_t, int> address_to_id;
    while(std::getline(ifs, line)) {
        
        #ifdef DEBUG
        std::cout << "Log:" << line << std::endl;
        #endif    
    
        std::stringstream ss(line);
        std::stringstream ss_sub;

        // read date
        std::tm t = {};
        std::getline(ss, item, ',');
        ss_sub = std::stringstream(item);
        ss_sub >> std::get_time(&t, "%Y%m%d%H%M%S");

        #ifdef DEBUG        
        std::cout << " - Time:" << std::put_time(&t, FTIME);
        std::cout << std::endl;
        #endif

        // read address
        std::getline(ss, item, '/');
        std::uint32_t address = address_to_int(item);
        std::getline(ss, item, ',');
        int mask = std::stoi(item);
        if (address_to_id.find(address) == address_to_id.end()) {

            // new id
            id = servers.size();
            servers.push_back(Server());
            address_to_id[address] = id;

            // set address
            servers[id].address = address;

            // set subnet address
            servers[id].subnet = calc_subnet(address, mask);

            #ifdef DEBUG
            std::cout << " - Address:" << address << ", ";
            std::cout << address_to_str(address) << std::endl;
            std::cout << " - Subnet:" << servers[id].subnet << ", ";
            std::cout << address_to_str(0xffffffff << (32-mask)) << ", ";
            std::cout << address_to_str(servers[id].subnet) << std::endl;
            #endif

        } else {
            id = address_to_id[address];
        }

        servers[id].dates.push_back(t);

        // read response time
        std::getline(ss, item);
        if (item == "-")
            servers[id].responses.push_back(-1);
        else
            servers[id].responses.push_back(std::stoi(item));

        #ifdef DEBUG
        std::cout << " - Response:" << servers[id].responses.back();
        std::cout << std::endl;
        #endif

    }
}

// 読み込んだサーバーの書き出し
// debug用
void print_all(const std::vector<Server> &servers) {
    for (const Server & server: servers) {
        std::cout << "Address:" << address_to_str(server.address) << std::endl;
        for (int i = 0; i < server.dates.size(); ++i) {
            std::cout << "\tDate:" << std::put_time(&server.dates[i], FTIME);
            std::cout << "\tResponse:" << server.responses[i];
            std::cout << std::endl;
        }
        std::cout << std::endl;
    }
}

// 課題1, 2, 3
// 故障or過負荷サーバーを列挙
void print_failures(std::vector<Server> &servers, int N, float t, int m) 
{

    for (Server &server: servers) {
        std::vector<Event> failed_log;
        std::vector<Event> overload_log;
        calc_failure(failed_log, server, N);
        if (m != 0)
            calc_overload(overload_log, server, t, m);
            
        if (failed_log.empty() && overload_log.empty())
            continue;

        std::cout << "Address:" << address_to_str(server.address) << std::endl;
        int i = 0;
        for (Event &e: failed_log) {
            std::cout << "\t" << e.text << std::endl;
        }
        for (Event &e: overload_log) {
            std::cout << "\t" << e.text << std::endl;
        }

        // std::cout << std::endl;
    }
}

// 課題1, 2, 4
// 故障を検出
void calc_failure(
    std::vector<Event> &log, Server &server, int N)
{
    
    bool failed = false;
    int count;
    std::tm start;
    for (int i = 0; i < server.dates.size(); ++i) {


        // 故障していて応答なし -> カウント
        if (failed && server.responses[i] == -1) {
            ++count; 
            continue;
        }
        
        // 稼働していて応答あり -> なにもしない
        if (!failed && server.responses[i] != -1) {
            continue;
        }

        // 稼働していて応答なし -> 故障日を記録, カウント開始
        if (!failed && server.responses[i] == -1) {
            failed = true;
            start = server.dates[i];
            count = 1;
            continue;
        }

        // 故障していて応答あり -> カウントがN以上なら故障判定
        if (failed && server.responses[i] != -1) {

            if (count >= N) {
                std::stringstream ss;
                Event e;
                e.type = 0; // down
                e.start = start;
                e.end = server.dates[i];
                ss << "down: " << std::put_time(&start, FTIME);
                ss << " -- " << std::put_time(&server.dates[i], FTIME);
                ss << " (" << std::right << std::setw(5);
                ss << std::mktime(&server.dates[i]) - std::mktime(&start);
                ss << " [sec.], timed out: " << count << " times)";
                e.text = ss.str();
                log.push_back(e);
            }
            failed = false;
            continue;
        }
    }
}

// 課題3
// 過負荷状態のサーバーを検出
void calc_overload(std::vector<Event> &log, Server &server,
    float t, int m) 
{
    
    bool is_overloaded = false;
    std::tm start;
    for (int i = 0; i < server.dates.size(); ++i) {

        // 負荷性計算
        int count = 0;
        float total_response = 0.0;
        for (int j = 0; i-j>=0; ++j) {
            
            // レスポンスがない場合はskip
            if (server.responses[i-j] == -1)
                continue;

            total_response += server.responses[i-j];
            ++count;

            #ifdef DEBUG
            std::cout << " - (" << total_response << "," << j<<","<<i-j << ")-> ";
            #endif

            if (count == m) break;
        }

        // 一度も応答が無い場合はtotal_response=0とする
        if (count != 0)
            total_response /= count;


        #ifdef DEBUG
        std::cout << " - " << std::put_time(&server.dates[i], FTIME) << " :";
        std::cout << total_response << ":" << count << std::endl;
        #endif


        // 負荷なし -> 負荷なし
        if (!is_overloaded && total_response <= t) continue;

        // 負荷あり -> 負荷あり
        if (is_overloaded && total_response > t) continue;

        // 負荷なし -> 負荷あり
        if (!is_overloaded && total_response > t) {
            is_overloaded = true;
            start = server.dates[i];
            continue;
        }

        // 負荷あり -> 負荷なし
        if (is_overloaded && total_response <= t) {
            std::stringstream ss;
            Event e;
            e.start = start;
            e.end = server.dates[i];
            ss << "overload: " << std::put_time(&start, FTIME);
            ss << " -- " << std::put_time(&server.dates[i], FTIME);
            ss << " (" << std::right << std::setw(5);
            ss << std::mktime(&server.dates[i]) - std::mktime(&start);
            ss << " [sec.])";
            e.text = ss.str();
            log.push_back(e);
            is_overloaded = false;
            continue;
        }
    }
}

// 数値アドレスを WWW.XXX.YYY.ZZZ へ変換
std::string address_to_str(std::uint32_t address) {
    std::stringstream ss;
    ss << (address>>24) << ".";
    ss << ((address>>16) & 0x000000ff) << ".";
    ss << ((address>>8)  & 0x000000ff) << ".";
    ss << (address & 0x000000ff);
    return ss.str();
}


// WWW.XXX.YYY.ZZZ を 数値へ変換
std::uint32_t address_to_int(const std::string &address) {
    std::stringstream ss(address);
    std::uint32_t address_int = 0;
    std::string item;
    std::getline(ss, item, '.');
    address_int += std::stoi(item) << 24;
    std::getline(ss, item, '.');
    address_int += std::stoi(item) << 16;
    std::getline(ss, item, '.');
    address_int += std::stoi(item) << 8;
    std::getline(ss, item);
    address_int += std::stoi(item);
    
    return address_int;
}


// subnetでマスクしたアドレスを返す
std::uint32_t calc_subnet(std::uint32_t address, int mask) {
    std::uint32_t subnet = address & (0xffffffff << (32-mask));
    return subnet;
}


// subnetを集計
void calc_networks(std::vector<Server> &servers, std::vector<std::vector<int>> &networks) {
    std::unordered_map<std::uint32_t, int> subnet_to_id;
    int id;

    for (int i = 0; i < servers.size(); ++i) {
        std::uint32_t subnet = servers[i].subnet;
        if (subnet_to_id.find(subnet) == subnet_to_id.end()) {
            id = networks.size();
            subnet_to_id[subnet] = id;
            networks.resize(networks.size() + 1);
        } else {
            id = subnet_to_id[subnet];
        }
        servers[i].subnet_id = id;
        networks[id].push_back(i);
    }
}

// subnetを列挙
// debug用
void print_networks(std::vector<Server> &servers, std::vector<std::vector<int>> &networks) {

    for (const std::vector<int> & network : networks) {
        std::cout << "Netwrok:" << address_to_str(servers[network[0]].subnet);
        std::cout << std::endl;
        for (int id : network ) {
            std::cout << "\t" << address_to_str(servers[id].address) << std::endl;
        }
    }
}

// 課題 4
// subnetの故障を検出
void print_network_failures(std::vector<Server> &servers, std::vector<std::vector<int>> &networks, int N) {

    for (int i = 0; i < networks.size(); ++i) {

        // subnet内のそれぞれのserverに対して, ログ計算
        std::vector<Event> all_events;
        for (int j = 0; j < networks[i].size(); ++j) {
            std::vector<Event> events;
            calc_failure(events, servers[networks[i][j]], N);
            for (int k = 0; k < events.size(); ++k) {
                events[k].server_id_in_subnet = j;
            }
            all_events.insert(all_events.end(), events.begin(), events.end());

            #ifdef DEBUG
            std::cout << "Address:";
            std::cout << address_to_str(servers[networks[i][j]].address);
            std::cout << " has " << events.size() << " events.";
            std::cout << " total:" << all_events.size() << std::endl;
            #endif
        }

        // network内のeventをmapで開始時刻でソート
        std::map<time_t, std::pair<int, int>> time_to_event;
        for (int j = 0; j < all_events.size(); ++j) {
            time_to_event[std::mktime(&all_events[j].start)] = std::make_pair(0, j);
            time_to_event[std::mktime(&all_events[j].end)] = std::make_pair(1, j);
        }

        #ifdef DEBUG
        std::cout << "Network:" << address_to_str(servers[networks[i][0]].subnet);
        std::cout << " - ";
        std::cout << time_to_event.size() << " events detected." << std::endl;
        for (auto &p : time_to_event) {
            std::cout << " - server:";
            std::cout << all_events[p.second.second].server_id_in_subnet;
            std::cout << " - " << all_events[p.second.second].text;
            std::cout << std::endl;
        }
        std::cout << std::endl;
        #endif


        // 故障を検査
        std::vector<bool> failed(networks[i].size());
        std::vector<Event> net_events;
        bool net_failed = false;
        std::tm start;
        for (auto &p: time_to_event) {

            int event_id = p.second.second;
            int server_id = all_events[event_id].server_id_in_subnet;

            #ifdef DEBUG
            std::cout << " state:" << net_failed;
            std::cout << " server:" << server_id;
            std::cout << " start or end:";
            std::cout << p.second.first;
            std::cout << "  " << all_events[event_id].text;
            std::cout << std::endl;
            #endif

            // end or start
            if (!p.second.first) { // start
                failed[server_id] = true;
            } else { // end
                failed[server_id] = false;
            }


            // check all server down or not
            bool new_failed = true;
            for (bool failed_i : failed) new_failed &= failed_i;

            #ifdef DEBUG
            std::cout << " -- judge";
            for (bool failed_i : failed) std::cout << " " << failed_i;
            std::cout << " -> " << new_failed << std::endl;
            #endif

            // 故障 -> 故障
            if (net_failed && new_failed) continue;

            // 正常 -> 正常
            if (!net_failed && !new_failed) continue;


            // 正常 -> 故障
            if (!net_failed && new_failed) {
                start = all_events[event_id].start;
                net_failed = true;

                #ifdef DEBUG
                std::cout << " -- down\n";
                #endif
                
                continue;
            }


            // 故障 -> 正常
            if (net_failed && !new_failed) {
                net_failed = false;

                std::stringstream ss;
                Event e;
                e.start = start;
                e.end = all_events[event_id].end;
                ss << "subnet down: " << std::put_time(&start, FTIME);
                ss << " -- " << std::put_time(&e.end, FTIME);
                ss << " (" << std::right << std::setw(5);
                ss << std::mktime(&e.end) - std::mktime(&start);
                ss << " [sec.])";
                e.text = ss.str();
                net_events.push_back(e);

                #ifdef DEBUG
                std::cout << " -- up\n";
                #endif
                
                continue;
            }
        }

        // 結果の出力
        if (!net_events.empty()) {
            std::cout << "Subnet Address:";
            std::cout << address_to_str(servers[networks[i][0]].subnet);
            std::cout << std::endl;
            for (auto &e : net_events) {
                std::cout << "\t" << e.text << std::endl;
            }
            // std::cout << std::endl;
        }
    }
}
