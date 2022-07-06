#ifndef DN_ERROR_LOG_H
#define DN_ERROR_LOG_H

#include "dn_cycle.h"

int dn_error_log_init(cycle_t *cycle);
int dn_error_log_release(cycle_t *cycle);
void dn_log_paxos(const int level, const char *fmt...);
#endif

