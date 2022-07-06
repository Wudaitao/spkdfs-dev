#include <dirent.h>
#include <dfs_dbg.h>
#include <args.hxx>
#include <dfs_utils.h>
#include <netinet/in.h>
#include "nn_main.h"
#include "../../etc/config.h"
#include "dfs_conf.h"
#include "nn_cycle.h"
#include "nn_signal.h"
#include "nn_module.h"
#include "nn_process.h"
#include "nn_conf.h"
#include "nn_time.h"
#include "nn_group.h"

std::string DEFAULT_CONF_FILE;

#define PATH_LEN  256

int          dfs_argc;
char       **dfs_argv;

string_t     config_file;
static int   g_format = NGX_FALSE;
static int   test_conf = NGX_FALSE;
static int   g_reconf = NGX_FALSE;
static int   g_quit = NGX_FALSE;
static int   show_version;
sys_info_t   dfs_sys_info;
extern pid_t process_pid;
std::string  ot_paxos_string;
static int parse_cmdline(int argc, char *const *argv);
static int conf_syntax_test(cycle_t *cycle);
static int sys_set_limit(uint32_t file_limit, uint64_t mem_size);
static int sys_limit_init(cycle_t *cycle);
static int format(cycle_t *cycle);
static int clear_current_dir(cycle_t *cycle);
static int save_ns_version(cycle_t *cycle);
static int get_ns_version(cycle_t *cycle);
static void init_conf_from_cmd(cycle_t * cycle);

void init_conf_from_cmd(cycle_t * cycle) {
    char localAddr[25];
    char localIp[INET_ADDRSTRLEN];
    cycle->ot_paxos_string = strdup(ot_paxos_string.c_str());
    get_local_ip(localIp);
    sprintf(localAddr,"%s:%d",localIp,NN_PAXOS_PORT);

    // set config
    nn_conf_set_my_paxos(localAddr,strlen(localAddr));
    nn_conf_set_ot_paxos(cycle);


}

static void dfs_show_help(void)
{
    printf("\t -c, Configure file\n"
		"\t -f, format the DFS filesystem\n"
        "\t -v, Version\n"
        "\t -t, Test configure\n"
        "\t -r, Reload configure file\n"
        "\t -q, stop namenode server\n");

    return;
}

static int parse_cmdline( int argc, char *const *argv)
{
    int ch = 0;
    char buf[255] = {0};
    args::ArgumentParser parser("namenode usage:", "end");
    args::HelpFlag help(parser, "help", " Display this help menu", {'h', "help"});
    args::ValueFlag<std::string> config(parser, "config", " Configure file", {'c'});
    args::Flag format(parser, "format", " format the file system ", {'f'});
    args::ValueFlag<std::string> ot_paxos_string_input(parser, "ot_paxos_string_input", " intial paxos ot_paxos_string", {'l'});
    args::Flag version(parser, "version", " version", {'v', 'V'});
    args::Flag test(parser, "test", " test the config file", {'t'});
    args::Flag reload(parser, "reload", " reload the config file", {'r'});
    args::Flag quit(parser, "quit", " quit namenode server", {'q'});

//    dbg(argc);
//    dbg(argv[0]);
//    dbg(argv[1]);
//    dbg(argv[2]);

    try {
        parser.ParseCLI(argc, argv);
    }
    catch (args::Help) {
        std::cout << parser;
        return NGX_ERROR;
    }
    catch (args::ParseError e) {
        std::cerr << e.what() << std::endl;
        std::cerr << parser;
        return NGX_ERROR;
    }
    catch (args::ValidationError e) {
        std::cerr << e.what() << std::endl;
        std::cerr << parser;
        return NGX_ERROR;
    }

    if (config) {
        string configFile = args::get(config);
        std::cout << args::get(config) << std::endl;
        if (configFile.empty()) {
            return NGX_ERROR;
        }
        if (configFile[0] == '/') {
            config_file.data = (uchar_t *) strdup(optarg);
            config_file.len = strlen(optarg);
        } else {
            getcwd(buf, sizeof(buf));
            buf[strlen(buf)] = '/';
            strcat(buf, configFile.c_str());
            config_file.data = (uchar_t *) strdup(buf);
            config_file.len = strlen(buf);
        }
        return NGX_OK;
    }
    if(format){
        g_format = NGX_TRUE;
        return NGX_OK;
    }
    if(ot_paxos_string_input){
        string ot_paxos_strings = args::get(ot_paxos_string_input);
//        if(!nodelists.empty()){
//            if(parse_ipport_list(nodelists.c_str(),nn_group->NodeList)==NGX_OK){
//                return NGX_OK;
//            }
//        }
        ot_paxos_string = ot_paxos_strings;
        return NGX_OK;
    }
    if(version){
        printf("namenode version: \"PACKAGE_STRING\"\n");
#ifdef CONFIGURE_OPTIONS
        printf("configure option:\"CONFIGURE_OPTIONS\"\n");
#endif
        show_version = 1;

        exit(0);

    }
    if(test){
        test_conf = NGX_TRUE;
        return NGX_OK;
    }
    if(reload){
        g_reconf= NGX_TRUE;
        return NGX_OK;
    }
    if(quit){
        g_quit = NGX_TRUE;
        return NGX_OK;
    }

    std::cout << parser;
    return NGX_ERROR;

}

int main(int argc, char **argv)
{
    int            ret = NGX_OK;
    cycle_t       *cycle = nullptr;
    conf_server_t *sconf = nullptr;

    cycle = cycle_create();

    time_init();

    std::string prefix = expand_user("~") +  PREFIX;
    DEFAULT_CONF_FILE =  prefix + "/etc/namenode.conf";

    nn_group = DfsGroup::getInstance("nn_group");
    if (parse_cmdline(argc, argv) != NGX_OK)
	{
        return NGX_ERROR;
    }

    if (!show_version && sys_get_info(&dfs_sys_info) != NGX_OK)
	{
        return NGX_ERROR;
    }


    if (config_file.data == nullptr)
	{
        config_file.data = (uchar_t *) strndup(DEFAULT_CONF_FILE.c_str(),
                strlen(DEFAULT_CONF_FILE.c_str()));
        config_file.len = strlen(DEFAULT_CONF_FILE.c_str());
    }
    
    if (test_conf == NGX_TRUE)
	{
        ret = conf_syntax_test(cycle);
		if(ret==NGX_OK){
		    printf("test config file successful\n");
		}
        goto out;
    }
    
    if (g_reconf || g_quit) 
	{
        if ((ret = cycle_init(cycle))!= NGX_OK)
		{
            fprintf(stderr, "cycle_init fail\n");
			
            goto out;
        }
		
        process_pid = process_get_pid(cycle);
        if (process_pid < 0)
		{
            fprintf(stderr, " get server pid fail\n");
            ret = NGX_ERROR;
			
            goto out;
        }

        if (g_reconf) 
		{
            int res = kill(process_pid, SIGNAL_RECONF);

            printf("config is reloaded %d\n",res);
        }
		else 
		{
            int res = kill(process_pid, SIGNAL_QUIT);
            printf("service is stoped, res:%d\n",res);
        }
		
        ret = NGX_OK;
		
        goto out;
    }

    umask(0022);
    // init conf 
    if ((ret = cycle_init(cycle)) != NGX_OK)
	{
        fprintf(stderr, "cycle_init fail\n");
		
        goto out;
    }
    // format the file system
    // init namespace id
	if (g_format) 
	{
	    format(cycle);
		
        goto out;
	}
    // set some params;
    init_conf_from_cmd(cycle);
    //

    if ((ret = process_check_running(cycle)) == NGX_TRUE)
	{
        fprintf(stderr, "namenode is already running\n");
		
    	goto out;
    }

    if ((ret = sys_limit_init(cycle)) != NGX_OK)
	{
        fprintf(stderr, "sys_limit_init error\n");
		
    	goto out;
    }
    
	ngx_module_setup();
    // just nn_error_log_init
	if ((ret = ngx_module_master_init(cycle)) != NGX_OK)
	{
		fprintf(stderr, "master init fail\n");
		
        goto out;
	}
    
    sconf = (conf_server_t *)cycle->sconf;

    if (process_change_workdir(&sconf->coredump_dir) != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
			"process_change_workdir failed!");
		
        goto failed;
    }

    if (signal_setup() != NGX_OK)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
			"setup signal failed!");
		
        goto failed;
    }
    // 守护进程
//    if (sconf->daemon == NGX_TRUE && nn_daemon() == NGX_ERROR)
//	{
//        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
//			"dfs_daemon failed");
//
//        goto failed;
//    }

    process_pid = getpid();
	
    if (process_write_pid_file(process_pid) == NGX_ERROR)
	{
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
			"write pid file error");
		
        goto failed;
    }
    // namespaceid is dfs_current_msec
	if (get_ns_version(cycle) != NGX_OK)
	{
	    dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, 
			"the DFS filesystem is unformatted");

        goto failed;
	}

    dfs_argc = argc;
    dfs_argv = argv;


    //
    ngx_master_process_cycle(cycle, dfs_argc, dfs_argv);

    process_del_pid_file();

failed:
    ngx_module_master_release(cycle);

out:
    if (config_file.data) 
	{
        free(config_file.data);
        config_file.len = 0;
    }
	
    if (cycle) 
	{
        cycle_free(cycle);
    }

    return ret;
}

int nn_daemon()
{
    int fd = NGX_INVALID_FILE;
    int pid = NGX_ERROR;
	
    pid = fork();

    if (pid > 0) 
	{
        exit(0);
    } 
	else if (pid < 0) 
	{
        printf("dfs_daemon: fork failed\n");
		
        return NGX_ERROR;
    }
	
    if (setsid() == NGX_ERROR)
	{
        printf("dfs_daemon: setsid failed\n");
		
        return NGX_ERROR;
    }

    umask(0022);

    fd = open("/dev/null", O_RDWR);
    if (fd == NGX_INVALID_FILE)
	{
        return NGX_ERROR;
    }

    if (dup2(fd, STDIN_FILENO) == NGX_ERROR)
	{
        printf("dfs_daemon: dup2(STDIN) failed\n");
		
        return NGX_INVALID_FILE;
    }

    if (dup2(fd, STDOUT_FILENO) == NGX_ERROR)
	{
        printf("dfs_daemon: dup2(STDOUT) failed\n");
		
        return NGX_ERROR;
    }

    if (dup2(fd, STDERR_FILENO) == NGX_ERROR)
	{
        printf("dfs_daemon: dup2(STDERR) failed\n");
		
        return NGX_ERROR;
    }

    if (fd > STDERR_FILENO) 
	{
        if (close(fd) == NGX_ERROR)
		{
            printf("dfs_daemon: close() failed\n");
			
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

static int conf_syntax_test(cycle_t *cycle)
{
    if (config_file.data == nullptr)
	{
        return NGX_ERROR;
    }

    if (cycle_init(cycle) == NGX_ERROR)
	{
        return NGX_ERROR;
    }
	
    return NGX_OK;
}

static int sys_set_limit(uint32_t file_limit, uint64_t mem_size)
{
    int            ret = NGX_ERROR;
    int            need_set = NGX_FALSE;
    struct rlimit  rl;
    log_t         *log;

    log = dfs_cycle->error_log;

    ret = getrlimit(RLIMIT_NOFILE, &rl);
    if (ret == NGX_ERROR)
	{
        dfs_log_error(log, DFS_LOG_ERROR,
            errno, "sys_set_limit get RLIMIT_NOFILE error");
		
        return ret;
    }
	
    if (rl.rlim_max < file_limit) 
	{
        rl.rlim_max = file_limit;
        need_set = NGX_TRUE;
    }
	
    if (rl.rlim_cur < file_limit) 
	{
        rl.rlim_cur = file_limit;
        need_set = NGX_TRUE;
    }
	
    if (need_set) 
	{
        ret = setrlimit(RLIMIT_NOFILE, &rl);
        if (ret == NGX_ERROR)
		{
            dfs_log_error(log, DFS_LOG_ERROR,
                errno, "sys_set_limit set RLIMIT_NOFILE error");
			
            return ret;
        }
    }

    // set mm overcommit policy to use large block memory
    if (mem_size > ((size_t)1 << 32)) 
	{
        ret = system("sysctl -w vm.overcommit_memory=1 > /dev/zero");
        if (ret == NGX_ERROR)
		{
            dfs_log_error(log, DFS_LOG_ERROR,
                errno, "sys_set_limit set vm.overcommit error");
			
            return NGX_ERROR;
        }
    }

    // enable core dump
    if (prctl(PR_SET_DUMPABLE, 1, 0, 0, 0) != 0) 
	{
        dfs_log_error(log, DFS_LOG_ERROR,
            errno, "sys_set_limit set PR_SET_DUMPABLE error");
		
        return NGX_ERROR;
    }
	
    if (getrlimit(RLIMIT_CORE, &rl) == 0) 
	{
        rl.rlim_cur = rl.rlim_max;
		
        ret = setrlimit(RLIMIT_CORE, &rl);
        if (ret == NGX_ERROR)
		{
            dfs_log_error(log, DFS_LOG_ERROR,
                errno, "sys_set_limit set RLIMIT_CORE error");
			
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

static int sys_limit_init(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;
    
    return sys_set_limit(sconf->connection_n, 0);
}

// format the filesystem 
static int format(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;

	fprintf(stdout, "Re-format filesystem in %s ? (Y or N)\n", 
		sconf->fsimage_dir.data);

	char input;
	scanf("%c", &input);

	if (input == 'Y' || input == 'y')
	{
		if (clear_current_dir(cycle) != NGX_OK)
		{
            return NGX_ERROR;
		}

		// namespace id is dfs_current_msec
		if (save_ns_version(cycle) != NGX_OK)
		{
            return NGX_ERROR;
		}

		fprintf(stdout, "Storage directory %s has been successfully formatted.\n", 
		    sconf->fsimage_dir.data);
	}
	else 
	{
        fprintf(stdout, "Format aborted in %s\n", sconf->fsimage_dir.data);
	}
	
    return NGX_OK;
}


// format 1
static int clear_current_dir(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;
	
    char dir[PATH_LEN] = {0};
	string_xxsprintf((uchar_t *)dir, "%s/current", sconf->fsimage_dir.data);

	if (access(dir, F_OK) != NGX_OK)
	{
	    if (mkdir(dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH) != NGX_OK)
	    {
	        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
				"mkdir %s err", dir);
		
	        return NGX_ERROR;
	    }

		return NGX_OK;
	}
	
	DIR *dp = nullptr;
    struct dirent *entry = nullptr;
	
    if ((dp = opendir(dir)) == nullptr)
	{
        printf("open %s err: %s\n", dir, strerror(errno));
		
        return NGX_ERROR;
    }

	while ((entry = readdir(dp)) != nullptr)
	{
		if (entry->d_type == 8)
		{
		    // file
            char sBuf[PATH_LEN] = {0};
 		    sprintf(sBuf, "%s/%s", dir, entry->d_name);
 		    printf("unlink %s\n", sBuf);
 
 		    unlink(sBuf);
		}
		else if (0 != strcmp(entry->d_name, ".") 
			&& 0 != strcmp(entry->d_name, ".."))
		{
            // sub-dir
		}
    }
	
	closedir(dp);
		
    return NGX_OK;
}

static int save_ns_version(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;

	char v_name[PATH_LEN] = {0};
	string_xxsprintf((uchar_t *)v_name, "%s/current/VERSION", 
		sconf->fsimage_dir.data);
	
	int fd = open(v_name, O_RDWR | O_CREAT | O_TRUNC, 0664);
	if (fd < 0) 
	{
		printf("open %s err: %s\n", v_name, strerror(errno));
		
        return NGX_ERROR;
	}

	int64_t namespaceID = time_curtime();

	char ns_version[256] = {0};
	sprintf(ns_version, "namespaceID=%ld\n", namespaceID);

	if (write(fd, ns_version, strlen(ns_version)) < 0) 
    {
		printf("write %s err: %s\n", v_name, strerror(errno));

		return NGX_ERROR;
	}

	close(fd);
	
    return NGX_OK;
}

static int get_ns_version(cycle_t *cycle)
{
    conf_server_t *sconf = (conf_server_t *)cycle->sconf;

	char v_name[PATH_LEN] = {0};
	string_xxsprintf((uchar_t *)v_name, "%s/current/VERSION", 
		sconf->fsimage_dir.data);
	
	int fd = open(v_name, O_RDWR|O_CREAT,0777);
	if (fd < 0)   
	{
		dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno, 
			"open %s err, maybe use namenode -h , to see how to format dfs", v_name);
		
        return NGX_ERROR;
	}

    char ns_version[256] = {0};
	if (read(fd, ns_version, sizeof(ns_version)) < 0) 
    {
		dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, 
			"read %s err", v_name);

		close(fd);

		return NGX_ERROR;
	}

	sscanf(ns_version, "%*[^=]=%ld", &cycle->namespace_id);

	close(fd);
	
    return NGX_OK;
}

