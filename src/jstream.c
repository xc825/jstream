//records.bin: rec_number(4bytes), data_type(1byte), data_length(4bytes), data (string, integer, boolean)
//dictionary.bin: TLV tag, rec_number

#include <stdlib.h>
#include <limits.h>
#include <apr_file_io.h>
#include <apr_errno.h>
#include "cJSON.h"

#define TLV_INT 1
#define TLV_STR 2
#define TLV_BOOL 3

#define DATA_TYPE_INT "I"
#define DATA_TYPE_STR "S"
#define DATA_TYPE_BOOL "B"

#define BOOL_TRUE "T"
#define BOOL_FALSE "F"

int process_input(apr_file_t *file, apr_file_t *rec_file, apr_file_t *dict_file);
int parse_json(char *buf, apr_table_t *dict);
int add_to_dict(char *key, apr_table_t *dict, int *record_number);
int write_record(cJSON *json, int record_number, apr_file_t *rec_file);
int write_tlv(apr_file_t *file, int record_number, int type, cJSON *cjson);
int write_dict(apr_table_t *dict, apr_file_t *dict_file);

int main() {
    apr_pool_t *pool;
    apr_file_t *input_file;
    apr_file_t *rec_file;
    apr_file_t *dict_file;
    apr_status_t rv;

    char input_fname[] = "input.txt";
    char rec_fname[] = "records.bin";
    char dict_fname[] = "dictionary.bin";

    apr_initialize();
    apr_pool_create(&pool, NULL);

    rv = apr_file_open(&input_file, input_fname, APR_READ, APR_OS_DEFAULT, pool);
    if (rv != APR_SUCCESS) {
        char errorbuf[1024];
        apr_strerror(rv, errorbuf, sizeof(errorbuf));
        printf("Error opening input file [%s]: %s\n", input_fname, errorbuf);
        return 1;
    }
    else {
        printf("File [%s] opened successfully\n", input_fname);
    }

    rv = apr_file_open(&rec_file, rec_fname, APR_FOPEN_WRITE | APR_FOPEN_TRUNCATE | APR_FOPEN_CREATE | APR_FOPEN_BINARY, APR_OS_DEFAULT, pool);
    if (rv != APR_SUCCESS) {
        char errorbuf[1024];
        apr_strerror(rv, errorbuf, sizeof(errorbuf));
        printf("Error opening file [%s]: %s\n", rec_fname, errorbuf);
        return 1;
    }
    else {
        printf("File [%s] opened for records successfully\n", rec_fname);
    }

    rv = apr_file_open(&dict_file, dict_fname, APR_FOPEN_WRITE | APR_FOPEN_TRUNCATE | APR_FOPEN_CREATE | APR_FOPEN_BINARY, APR_OS_DEFAULT, pool);
    if (rv != APR_SUCCESS) {
        char errorbuf[1024];
        apr_strerror(rv, errorbuf, sizeof(errorbuf));
        printf("Error opening file [%s]: %s\n", dict_fname, errorbuf);
        return 1;
    }
    else {
        printf("File [%s] opened for dictionary successfully\n", dict_fname);
    }

    process_input(input_file, rec_file, dict_file);

    apr_file_close(input_file);
    apr_file_close(rec_file);
    apr_file_close(dict_file);

    apr_pool_destroy(pool);
    apr_terminate();

    return 0;
}

int process_input(apr_file_t *file, apr_file_t *rec_file, apr_file_t *dict_file) {
    apr_status_t rv;
    char buf[1024];
    cJSON *json;
    cJSON *json_tmp;
    apr_table_t *dict;
    apr_pool_t *pool;
    int record_number;

    apr_pool_create(&pool, NULL);
    dict = apr_table_make(pool, 0);

    while (APR_SUCCESS == (rv = apr_file_gets(buf, sizeof(buf), file))) {
        printf("parsing JSON line:%s", buf);
        json = cJSON_Parse(buf);
        if (json == NULL) {
            printf("Error parsing JSON:%s\n", buf);
            continue;
        }

        json_tmp = json->child;
        while (json_tmp) {
                if (apr_table_get(dict, json_tmp->string) == NULL) {
                    add_to_dict(json_tmp->string, dict, &record_number);
                    write_record(json_tmp, record_number, rec_file);
                }
                else {
                    printf("Duplicate key: %s, skip\n", json_tmp->string);
                }
            json_tmp = json_tmp->next;
        }
        cJSON_Delete(json);
    }

    printf("Write dictionary to file\n");
    write_dict(dict, dict_file);
    apr_pool_destroy(pool);

    return 0;
}

int add_to_dict(char *key, apr_table_t *dict, int *number) {
    static int record_number = 1;
    char str_num[64];

    *number = record_number;
    record_number++;
    snprintf(str_num, sizeof(str_num), "%d", record_number);
    printf("Add to dictionary - [key: %s, rec_number: %d]\n", key, record_number);
    apr_table_set(dict, key, str_num);

    return 0;
}

int write_record(cJSON *cjson, int record_number, apr_file_t *rec_file) {
//write TLV to file
    switch (cjson->type) {
        case cJSON_String:
            printf("Write to record file:[%d:%s]\n", record_number, cjson->valuestring);
            write_tlv(rec_file, record_number, TLV_STR, cjson);
            break;
        case cJSON_Number:
            printf("Write to record file:[%d:%d]\n", record_number, cjson->valueint);
            write_tlv(rec_file, record_number, TLV_INT, cjson);
            break;
        case cJSON_True:
        case cJSON_False:
            printf("Write to record file:[%d:%s]\n", record_number, (cjson->type & cJSON_True) ? "T" : "F");
            write_tlv(rec_file, record_number, TLV_BOOL, cjson);
            break;
        default:
            printf("Unknown type\n");
            break;
    }

    return 0;
}

//int,TLV: 4 bytes record number, 1 byte data type(tag), 4 bytes data length, data (string, integer, boolean)
int write_tlv(apr_file_t *file, int record_number, int type, cJSON *cjson) {
    apr_size_t len;
    apr_size_t data_len;

    switch (type) {
        case TLV_INT:
            len = sizeof(int);
            apr_file_write(file, &record_number, &len);
            len = sizeof(char);
            apr_file_write(file, DATA_TYPE_INT, &len);
            data_len = sizeof(int);
            apr_file_write(file, &data_len, &len);
            len = sizeof(int);
            apr_file_write(file, &cjson->valueint, &len);
            break;
        case TLV_STR:
            len = sizeof(int);
            apr_file_write(file, &record_number, &len);
            len = sizeof(char);
            apr_file_write(file, DATA_TYPE_STR, &len);
            data_len = strlen(cjson->valuestring);
            len = sizeof(int);
            apr_file_write(file, &data_len, &len);
            len = strlen(cjson->valuestring);
            apr_file_write(file, cjson->valuestring, &len);
            break;
        case TLV_BOOL:
            len = sizeof(int);
            apr_file_write(file, &record_number, &len);
            len = sizeof(char);
            apr_file_write(file, DATA_TYPE_BOOL, &len);
            data_len = sizeof(char);
            len = sizeof(char);
            apr_file_write(file, &data_len, &len);
            len = sizeof(char);
            if (cjson->type == cJSON_True) {
                apr_file_write(file, BOOL_TRUE, &len);
            }
            else if (cjson->type == cJSON_False) {
                apr_file_write(file, BOOL_FALSE, &len);
            }
            else {
                printf("Unknown boolean value\n");
                return 1;
            }
            break;
        default:
            printf("Unknown type\n");
            return 1;
    }

    return 0;
}

//tlvKEY,intVALUE
int write_dict(apr_table_t *dict, apr_file_t *dict_file) {
    apr_size_t len;
    apr_size_t key_len;
    char *value;
    int record_number;
    char *key;

    const apr_array_header_t* arr_header = apr_table_elts(dict);
    apr_table_entry_t *table = (apr_table_entry_t *)arr_header->elts;

    for (int i = 0; i < apr_table_elts(dict)->nelts; i++) {
        key = table[i].key;
        value = table[i].val;
        record_number = atoi(value);
        len = sizeof(int);
        apr_file_write(dict_file, &record_number, &len);

        //write TLV
        len = sizeof(char);
        apr_file_write(dict_file, DATA_TYPE_STR, &len); //write data type, tag, 1 byte
        key_len = strlen(key);
        len = sizeof(int);
        apr_file_write(dict_file, &key_len, &len); //write data length, integer 4 bytes
        len = key_len;
        apr_file_write(dict_file, key, &len); //write data, string
        len = sizeof(int);
        apr_file_write(dict_file, &record_number, &len); //write record number, integer 4 bytes
    }

    return 0;
}
