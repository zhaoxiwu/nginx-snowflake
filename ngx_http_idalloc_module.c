/***************************************************************************
 * 
 * Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/



/**
 * @file ngx_http_idalloc_module.c
 * @author zhaoxiwu(com@baidu.com)
 * @date 2014/11/24 17:47:09
 * @brief 
 *  
 **/

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_times.h>

typedef struct
{
    ngx_int_t idalloc_counter;
    ngx_int_t idalloc_datacenter;
}ngx_http_idalloc_loc_conf_t;

static ngx_int_t workerIdBits = 5;
static ngx_int_t datacenterIdBits = 5;
static ngx_int_t sequenceBits = 12;
static ngx_int_t sequenceId = 1024;
static ngx_int_t datacenterId = 1;

static long maxWorkerId = 1;
static long maxDatacenterId  = 1;
static long sequenceMask = 1;
static long twepoch = 1412761152649;
static long allocedId = 0;

static ngx_int_t workerIdLeftShift  = 0;
static ngx_int_t datacenterIdLeftShift = 0;
static ngx_int_t timestampLeftShit = 0; 

static ngx_msec_t lastTimestamp = 0;
static long workerId = 0;

static ngx_int_t ngx_http_idalloc_init(ngx_conf_t *cf);

static void *ngx_http_idalloc_create_loc_conf(ngx_conf_t *cf);

static char *ngx_http_idalloc_counter(ngx_conf_t *cf, ngx_command_t *cmd,
        void *conf);

static char *ngx_http_idalloc_datacenter(ngx_conf_t *cf, ngx_command_t *cmd,
        void *conf);

static ngx_command_t ngx_http_idalloc_commands[] = {
    {
        ngx_string("idalloc"),
        NGX_HTTP_LOC_CONF|NGX_CONF_FLAG,
        ngx_http_idalloc_counter,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_idalloc_loc_conf_t, idalloc_counter),
        NULL
    },
    { 
        ngx_string("datacenter"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_idalloc_datacenter,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_idalloc_loc_conf_t, idalloc_datacenter),
        NULL
    },
    ngx_null_command
};


static ngx_http_module_t ngx_http_idalloc_module_ctx = {
    NULL,                          /* preconfiguration */
    ngx_http_idalloc_init,           /* postconfiguration */

    NULL,                          /* create main configuration */
    NULL,                          /* init main configuration */

    NULL,                          /* create server configuration */
    NULL,                          /* merge server configuration */

    ngx_http_idalloc_create_loc_conf, /* create location configuration */
    NULL                            /* merge location configuration */
};


ngx_module_t ngx_http_idalloc_module = {
    NGX_MODULE_V1,
    &ngx_http_idalloc_module_ctx,    /* module context */
    ngx_http_idalloc_commands,       /* module directives */
    NGX_HTTP_MODULE,               /* module type */
    NULL,                          /* init master */
    NULL,                          /* init module */
    NULL,                          /* init process */
    NULL,                          /* init thread */
    NULL,                          /* exit thread */
    NULL,                          /* exit process */
    NULL,                          /* exit master */
    NGX_MODULE_V1_PADDING
};

ngx_msec_t genTime(ngx_http_request_t *r)
{
    struct timeval   tv; 
    ngx_gettimeofday(&tv); 
    ngx_msec_t timestamp = (ngx_msec_t) (tv.tv_sec*1000 + tv.tv_usec / 1000);

    //ngx_timeofday(); get cached time
    //ngx_time_t *tp = ngx_timeofday(); 
    //ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "genTime: sec:%l, msec%l",tp->sec, tp->msec);
    //ngx_msec_t timestamp = (ngx_msec_t) (tp->sec * 1000 + tp->msec);
    ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "genTime: timestamp:%l",timestamp);
    
    return timestamp;
}


ngx_msec_t tilNextMillis(ngx_http_request_t *r,ngx_msec_t lastTimestamp)
{
   ngx_msec_t timestamp = genTime(r);
    while(timestamp <= lastTimestamp)
    {
        ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "in til nex: timestamp:%l, last %l",timestamp, lastTimestamp);
        timestamp = genTime(r);  
    }

    return timestamp;
}
    static ngx_int_t
ngx_http_idalloc_handler(ngx_http_request_t *r)
{
    ngx_int_t    rc;
    ngx_buf_t   *b;
    ngx_chain_t  out;
    ngx_msec_t  now;
    ngx_http_idalloc_loc_conf_t* my_conf;
    u_char ngx_idalloc_string[1024] = {0};
    ngx_uint_t content_length = 0;

    ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "idalloc_handler is called!");

    my_conf = ngx_http_get_module_loc_conf(r, ngx_http_idalloc_module);

    ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "after get loc conf!");

    now = genTime(r); 

    if (my_conf->idalloc_datacenter == NGX_CONF_UNSET && my_conf->idalloc_counter != NGX_CONF_UNSET)  
    {

        ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "idalloc datacenter no set!!");
        return NGX_HTTP_NOT_ALLOWED;  
    }
    else{
        datacenterId = my_conf->idalloc_datacenter;
    }
    if (now < lastTimestamp)
    {
        return NGX_HTTP_NOT_ALLOWED;  
    }
  
   ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "now %l, last:%l",now,lastTimestamp); 
   if (now == lastTimestamp)
   {
       sequenceId = (sequenceId + 1) & sequenceMask;
       ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "sequenceId:%d",sequenceId); 
       if(sequenceId == 0)
       {
            ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "before ti next time");
            now = tilNextMillis(r,lastTimestamp);
            ngx_log_error(NGX_LOG_EMERG, r->connection->log, 0, "after ti next time");
       }
   }
   else{
        sequenceId = 0;
   }
    ngx_pid_t  pid =  ngx_getpid();
    workerId = (ngx_int_t) pid; 
    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "get  workerid:%d",workerId);
    allocedId = ((now - twepoch) << timestampLeftShit) | (datacenterId << datacenterIdLeftShift) | (workerId << workerIdLeftShift) | sequenceId;
    lastTimestamp = now;

    if (my_conf->idalloc_counter == NGX_CONF_UNSET
            || my_conf->idalloc_counter == 0)
    {
        return NGX_DECLINED;
    }
    else
    {
        ngx_sprintf(ngx_idalloc_string, "new id:%l,%P,%d", allocedId, pid, workerId);
    }
    content_length = ngx_strlen(ngx_idalloc_string);

    if (!(r->method & (NGX_HTTP_GET|NGX_HTTP_HEAD))) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "before discard requers body");
    rc = ngx_http_discard_request_body(r);

    if (rc != NGX_OK) {
        return rc;
    }

    ngx_str_set(&r->headers_out.content_type, "text/html");

    if (r->method == NGX_HTTP_HEAD) {
        r->headers_out.status = NGX_HTTP_OK;
        r->headers_out.content_length_n = content_length;

        return ngx_http_send_header(r);
    }

    b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
    if (b == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    out.buf = b;
    out.next = NULL;

    b->pos = ngx_idalloc_string;
    b->last = ngx_idalloc_string + content_length;
    b->memory = 1;    /* this buffer is in memory */
    b->last_buf = 1;  /* this is the last buffer in the buffer chain */

    r->headers_out.status = NGX_HTTP_OK;
    r->headers_out.content_length_n = content_length;

    rc = ngx_http_send_header(r);

    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }

    return ngx_http_output_filter(r, &out);
}

static void *ngx_http_idalloc_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_idalloc_loc_conf_t* local_conf = NULL;
    local_conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_idalloc_loc_conf_t));
    if (local_conf == NULL)
    {
        return NULL;
    }
  
    local_conf->idalloc_counter = NGX_CONF_UNSET;
    local_conf->idalloc_datacenter = NGX_CONF_UNSET;
    return local_conf;
}


static char *ngx_http_idalloc_counter(ngx_conf_t *cf, ngx_command_t *cmd,
        void *conf)
{
    ngx_http_idalloc_loc_conf_t* local_conf;

    local_conf = conf;

    char* rv = NULL;

    rv = ngx_conf_set_flag_slot(cf, cmd, conf);

    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "idalloc_counter:%d", local_conf->idalloc_counter);
    return rv;
}

static char *ngx_http_idalloc_datacenter(ngx_conf_t *cf, ngx_command_t *cmd,
        void *conf)
{
    ngx_http_idalloc_loc_conf_t* local_conf;

    local_conf = conf;

    char* rv = NULL;

    rv = ngx_conf_set_num_slot(cf, cmd, conf);
    if (rv != NGX_OK)
    {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "idalloc_datacenter no set");
    }
    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "idalloc_datacenter:%d", local_conf->idalloc_datacenter);
    return rv;
}
    static ngx_int_t
ngx_http_idalloc_init(ngx_conf_t *cf)
{
    ngx_http_handler_pt        *h;
    ngx_http_core_main_conf_t  *cmcf;

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    h = ngx_array_push(&cmcf->phases[NGX_HTTP_CONTENT_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    *h = ngx_http_idalloc_handler;
    maxWorkerId = 1 ^ (-1 << workerIdBits);
    maxDatacenterId  = 1 ^ (-1 << datacenterIdBits);
    sequenceMask = 1 ^ (-1 << sequenceBits);

    workerIdLeftShift  = sequenceBits;
    datacenterIdLeftShift = workerIdBits + sequenceBits;
    timestampLeftShit = sequenceBits + workerIdBits + datacenterIdBits; 
//    ngx_pid_t ngx_pid = ngx_getpid();  
//   workerId = (int)ngx_pid; 
//    ngx_conf_log_error(NGX_LOG_EMERG, cf, 0, "work id :%P", ngx_pid);
    return NGX_OK;
}


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
