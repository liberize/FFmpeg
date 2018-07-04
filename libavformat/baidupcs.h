#ifndef AVFORMAT_BAIDUPCS_H
#define AVFORMAT_BAIDUPCS_H

enum PCSUserAgentType {
    PCS_ANDROID_UA = 1,
    PCS_WIN_UA
};

char *pcs_get_user_agent(enum PCSUserAgentType type);
char *pcs_get_referer(void);
char *pcs_get_ssl_public_key(void);

#endif
