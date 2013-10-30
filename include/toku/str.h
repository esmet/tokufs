/**
 * TokuFS
 */

#ifndef TOKU_STR_H
#define TOKU_STR_H

#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "cc.h"

/**
 * Return an allocated copy of str
 */
UNUSED
static inline char * toku_strdup(const char * str)
{
    char * s = malloc(strlen(str) + 1);
    strcpy(s, str);
    return s;
}

/**
 * Count how many times c occurs in str
 */
UNUSED
static inline int toku_strcount(const char * str, char c)
{
    int count = 0;
    while (*str != '\0') {
        if (*str == c) {
            count++;
        }
        str++;
    }
    return count;
}

/**
 * True if str has the given prefix
 */
UNUSED
static inline int toku_strprefix(const char * str, const char * prefix)
{
    return strncmp(str, prefix, strlen(prefix)) == 0;
}

/**
 * Combine N strings into one new allocated string
 */
UNUSED
static inline char * toku_strcombine(char ** strings, int count) 
{
    int i, length;
    char * buf;

    length = 0;
    for (i = 0; i < count; i++) {
        length += (strlen(strings[i]) + 1);
    }
    length++;

    buf = malloc(sizeof(char) * (length + 1));
    memset(buf, 0, length + 1);
    for (i = 0; i < count; i++) {
        strcat(buf, strings[i]);
        strcat(buf, " ");
    }

    return buf;
}

/**
 * Get a string representing the current date.
 * The returned string needs to be freed by the caller.
 */
UNUSED
static char * toku_strdate(void)
{
    time_t now;
    char * date;
    char * newline;

    now = time(NULL);
    date = ctime(&now);
    date = toku_strdup(date);
    
    /* at least one ctime implementation is putting on its
     * own newline char, for some reason. */
    newline = strchr(date, '\n');
    if (newline != NULL) {
        *newline = '\0';
    }

    return date;
}

#endif /* TOKU_STR_H */
