#define FUSE_USE_VERSION 26
#define MAX_NUM_FILE 1000
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <fuse.h>
#include <sys/mman.h>
#include <stdio.h>

static const size_t size = 1024 * 1024 * (size_t)1024;
static const size_t blocksize = 4*1024;
static const size_t blocknr = 1024*256;
static void *mem[1024*256];
static int32_t *bitmap;//表示内存占用情况

struct FileNode {
    char filename[64];
    int32_t num_block;
    int32_t next_block;
    struct stat st;
    struct FileNode *next;
};

struct IndexNode
{
    struct FileNode* root;
    int32_t num_bitmap;//bitmap数组大小
    int32_t avail_block;//可用块数
}*indexnode;

static struct FileNode *get_FileNode(const char *name)
{
    struct FileNode *node = indexnode->root;
    while(node)
    {
        if(strcmp(node->filename, name + 1) != 0)
            node = node->next;
        else
            return node;
    }
    return NULL;
}

int32_t get_idle_block()
{
    int32_t free_mem = 0, num_bitmap_block = blocknr/(8*sizeof(int32_t));
    for(; (free_mem<num_bitmap_block)&&(bitmap[free_mem]==0xFFFFFFFF); free_mem++);
    if (free_mem == num_bitmap_block)
        return -ENOSPC;
    
    return free_mem*32+__builtin_ctzll(~bitmap[free_mem]);
}

void free_mem(int32_t block)
{
	memset(mem[block], 0, blocksize);
    munmap(mem[block], blocksize);
    indexnode->avail_block++;
    bitmap[block>>5] &= ~(1 << (block&0x1F));
}

static void create_FileNode(const char *filename, const struct stat *st)
{
    int32_t num_newnode = get_idle_block();
    mem[num_newnode] = mmap(NULL, blocksize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    memset(mem[num_newnode], 0, blocksize);
    struct FileNode *new = (struct FileNode *)mem[num_newnode];
    memcpy(new->filename, filename, strlen(filename)+1);
    memcpy(&new->st, st, sizeof(struct stat));
    new->next = indexnode->root;
    indexnode->root = new;
    indexnode->avail_block--;
    bitmap[num_newnode>>5] |= (1 << (num_newnode&0x1F));
    
    if (strcmp(filename, "/") != 0)
    {
        int32_t block = get_idle_block();
        mem[block] = mmap(NULL, blocksize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        memset(mem[block], 0, blocksize);
        indexnode->avail_block--;
        bitmap[block>>5] |= (1 << (block&0x1F));
        new->next_block = block;
        new->num_block++;
    }
}

static void *oshfs_init(struct fuse_conn_info *conn)
{
    //set indexnode
    mem[0] = mmap(NULL, blocksize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    memset(mem[0], 0, blocksize);
    indexnode = (struct IndexNode*)mem[0];
    indexnode->root = NULL;
    
    //set bitmap
    indexnode->num_bitmap = blocknr/(8*sizeof(int32_t));
    int32_t num_bitmap_block = blocknr/(8*blocksize);
    mem[1] = mmap(NULL, blocksize*num_bitmap_block, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    memset(mem[1], 0, blocksize*num_bitmap_block);
    for(int i=1; i<=num_bitmap_block; i++)
        mem[i] = (char*)mem[1]+blocksize*(i-1);
    bitmap = (int32_t *)mem[1];
    for(int i=0; i<=num_bitmap_block; i++)
        bitmap[i>>5] |= (1<<(i&0x01F));
    
    indexnode->avail_block = blocknr-num_bitmap_block-1;
    return NULL;
}

static int oshfs_getattr(const char *path, struct stat *stbuf)
{
    int ret = 0;
    struct FileNode *node = get_FileNode(path);
    if (strcmp(path, "/") == 0)
    {
        memset(stbuf, 0, sizeof(struct stat));
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_uid = fuse_get_context()->uid;
        stbuf->st_gid = fuse_get_context()->gid;
    }
    else if(node)
    {
        memcpy(stbuf, &node->st, sizeof(struct stat));
    }
    else
    {
        ret = -ENOENT;
    }
    return ret;
}

static int oshfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    struct FileNode *node = indexnode->root;
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    int i=0;
    while(node)
    {
        filler(buf, node->filename, &node->st, 0);
        node = node->next;
    }
    return 0;
}

static int oshfs_mknod(const char *path, mode_t mode, dev_t dev)
{
    struct stat st;
    st.st_mode = S_IFREG | 0644;
    st.st_uid = fuse_get_context()->uid;
    st.st_gid = fuse_get_context()->gid;
    st.st_nlink = 1;
    st.st_size = 0;
    create_FileNode(path + 1, &st);
    return 0;
}

static int oshfs_open(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

static int oshfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    struct FileNode *node = get_FileNode(path);
    int need_block = (offset+size-1)/(blocksize-4)+1-node->num_block;
    if (need_block > indexnode->avail_block)
        return -ENOSPC;
    else node->st.st_size = offset + size;
    
    int last_block = node->next_block;
    while (*((int*)mem[last_block]) != 0)
        last_block = *((int*)mem[last_block]);
    while (need_block > 0)
    {
        int32_t block = get_idle_block();
        mem[block] = mmap(NULL, blocksize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        memset(mem[block], 0, blocksize);
        indexnode->avail_block--;
        bitmap[block>>5] |= (1 << (block&0x1F));
        *((int*)mem[last_block]) = block;
        last_block = block;
        node->num_block++;
        need_block--;
    }
    
    
    int skip_block = offset/(blocksize-4);//前四个字节存放下一块位置
    int offset_block = offset%(blocksize-4);
    int num_block = (offset_block+size-1)/(blocksize-4);
    
    int start_block = node->next_block;
    for (int i=0; i<skip_block; i++)
        start_block = *((int*)mem[start_block]);
    if (num_block == 0)//只要写一块
        memcpy(mem[start_block]+4+offset_block, buf, size);
    else
    {
        int block = start_block;
        const char *wbuf = buf;
        while(block != 0)
        {
            if (block == start_block)
            {
                memcpy(mem[block]+4+offset_block, wbuf, blocksize-4-offset_block);
                wbuf += blocksize-4-offset_block;
            }
            else if (*((int*)mem[block]) != 0)
            {
                memcpy(mem[block]+4, wbuf, blocksize-4);
                wbuf += blocksize-4;
            }
            else
            {
                memcpy(mem[block]+4, wbuf, size-(int64_t)(wbuf-buf));
            }
            block = *((int*)mem[block]);
        }
    }
    
    return size;
}

static int oshfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    struct FileNode *node = get_FileNode(path);
    int64_t ret = size;
    if(offset + size > node->st.st_size)
        ret = (node->st.st_size - offset);
    int skip_block = offset / (blocksize-4);//每块用四个字节存放下一个块的位置
    int offset_block = offset % (blocksize-4);
    int num_block = (offset_block+ret-1) / (blocksize-4);
    
    int start_block = node->next_block;
    for (int i=0; i<skip_block; i++)
        start_block = *((int*)mem[start_block]);

    if (num_block == 0)//只要读一块
        memcpy(buf, mem[start_block]+4+offset_block, ret);
    else
    {
        int block = start_block, num = num_block;
        char *rbuf = buf;
        while (num >= 0)
        {
            if (num == num_block)
            {
                memcpy(rbuf, mem[block]+4+offset_block, blocksize-4-offset_block);
                rbuf += blocksize-4-offset_block;
            }
            else if (num > 0)
            {
                memcpy(rbuf, mem[block]+4, blocksize-4);
                rbuf += blocksize-4;
            }
            else
                memcpy(rbuf, mem[block]+4, ret-(int64_t)(rbuf-buf));
            block = *((int*)mem[block]);
			num--;

        }
    }
    return ret;
}

static int oshfs_unlink(const char *path)
{
    struct FileNode *node = get_FileNode(path), *root = indexnode->root;
    int start_block = node->next_block, block = node->next_block;
    while (block != 0)
    {
        block = *((int*)mem[block]);
        free_mem(start_block);
        start_block = block;
    }
    if (root == node)
	{
		root = root->next;
		indexnode->root = root;
	}
	else if (node)
	{
    	while ((root->next) != node)
			root = root->next;
        root->next = node->next;
	}

	for (block=0; mem[block]!=(void*)node; block++);
    free_mem(block);
    return 0;
}

static int oshfs_truncate(const char *path, off_t size)
{
    struct FileNode *node = get_FileNode(path);
    int remain_block = (size-1) / blocksize + 1;
    if (size == 0) remain_block = 1;
    
    int start_block = node->next_block;
    while (remain_block > 1)
    {
        start_block = *((int*)mem[start_block]);
        remain_block--;
    }
    
    int block = *((int*)mem[start_block]);
    *((int*)mem[start_block]) = 0;
    start_block = block;
    while (block != 0)
    {
        block = *((int*)mem[block]);
        free_mem(start_block);
        start_block = block;
    }
    
    node->st.st_size = size;
    return 0;
}

static const struct fuse_operations op =
{
    .init = oshfs_init,
    .getattr = oshfs_getattr,
    .readdir = oshfs_readdir,
    .mknod = oshfs_mknod,
    .open = oshfs_open,
    .write = oshfs_write,
    .truncate = oshfs_truncate,
    .read = oshfs_read,
    .unlink = oshfs_unlink,
};

int main(int argc, char *argv[])
{
    return fuse_main(argc, argv, &op, NULL);
}
