#include <libnet.h>
#include "dfs_conf.h"
#include "dfs_memory.h"
#include "dfscli_conf.h"
#include "dfscli_cycle.h"


#define CYCLE_POOL_SIZE 16384

clicycle_t         *dfscli_cycle;
extern string_t  config_file;
// 分配空间
clicycle_t *cycle_create()
{
    clicycle_t *cycle = nullptr;
    cycle = (clicycle_t *)memory_calloc(sizeof(clicycle_t));
    
    if (!cycle) 
	{
        printf("create cycle faild!\n");
		
        return cycle;
    }
    
    cycle->pool = pool_create(CYCLE_POOL_SIZE, CYCLE_POOL_SIZE, nullptr);
    if (!cycle->pool) 
	{
        memory_free(cycle, sizeof(clicycle_t));
        cycle = nullptr;
    }
    
    return cycle;
}

int cycle_init(clicycle_t *cycle)
{
    log_t          *log = nullptr;
    conf_context_t *ctx = nullptr;
    conf_object_t  *conf_objects = nullptr;
    string_t        server = string_make("Server");
    
    if (cycle == nullptr)
	{
        return NGX_ERROR;
    }
  	// 配置文件相对于安装目录的路径名称
    cycle->conf_file.data = string_xxpdup(cycle->pool , &config_file);
    cycle->conf_file.len = config_file.len;
    
    if (!dfscli_cycle)
	{
        log = error_log_init_with_stderr(cycle->pool);
        if (!log) 
		{
            goto error;
        }
		
        cycle->error_log = log;
        cycle->pool->log = log;
        dfscli_cycle = cycle;
    }
	
    error_log_set_handle(log, nullptr, nullptr);
 
    ctx = conf_context_create(cycle->pool);//上下文从pool为conf_ctx分配空间
    if (!ctx) 
	{
        goto error;
    }
    
    conf_objects = get_dn_conf_object();

	// ctx->conf_file = config_file ,ctx->conf_obj=conf_obj
    if (conf_context_init(ctx, &config_file, log, conf_objects) != NGX_OK)
	{
        goto error;
    }
    // 配置文件解析
    if (conf_context_parse(ctx) != NGX_OK)
	{
        printf("configure parse failed at line %d\n", ctx->conf_line); 
		
        goto error;
    }
    
    cycle->sconf = conf_get_parsed_obj(ctx, &server);
    if (!cycle->sconf) 
	{
        printf("no Server conf\n");
		
        goto error;
    }
    return NGX_OK;
    
error:
     return NGX_ERROR;
}

int cycle_free(clicycle_t *cycle)
{
    if (!cycle) 
	{
        return NGX_OK;
    }
   
    if (cycle->pool) 
	{
        pool_destroy(cycle->pool);
    }
    
    memory_free(cycle, sizeof(clicycle_t));

    cycle = nullptr;

    return NGX_OK;
}

// 默认两张网卡的话用第一张
static int nn_get_local_ip(char *ip) {
    int fd, intrface, retn = 0;
    struct ifreq buf[INET_ADDRSTRLEN];
    struct ifconf ifc{};
    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) >= 0) {
        ifc.ifc_len = sizeof(buf);
        // caddr_t,linux内核源码里定义的：typedef void *caddr_t；
        ifc.ifc_buf = (caddr_t) buf;
        if (!ioctl(fd, SIOCGIFCONF, (char *) &ifc)) {
            intrface = ifc.ifc_len / sizeof(struct ifreq);
            //
            if (intrface > 1) {
                dfs_log_error(dfscli_cycle->error_log, DFS_LOG_DEBUG, 0, "you have used multiple net interface card,"
                                                                      "default use the second for program.(maybe not 127.0.0.1)");
                strcpy(ip, inet_ntoa(((struct sockaddr_in *) (&buf[1].ifr_addr))->sin_addr));
                close(fd);

                return NGX_OK;
            } else if (intrface == 1) {
                dfs_log_error(dfscli_cycle->error_log, DFS_LOG_ERROR, 0,
                              "detect only one net interface card,will use 127.0.0.1");
                strcpy(ip, inet_ntoa(((struct sockaddr_in *) (&buf[0].ifr_addr))->sin_addr));
                close(fd);

                return NGX_OK;
            } else {
                dfs_log_error(dfscli_cycle->error_log, DFS_LOG_ERROR, 0, "detect no net interface card , exit!");
                close(fd);
                return NGX_ERROR;
            }

            //
//            while (intrface-- > 0) {
//                    strcpy(ip, inet_ntoa(((struct sockaddr_in *) (&buf[intrface].ifr_addr))->sin_addr));
////                    ip=(inet_ntoa(((struct sockaddr_in*)(&buf[intrface].ifr_addr))->sin_addr));
//                    printf("IP:%s\n", ip);
//                }
//            }
        }
    }
    close(fd);
    dfs_log_error(dfscli_cycle->error_log, DFS_LOG_ERROR, 0, "get_local_ip func error");
    return NGX_ERROR;
}

