2024-3-8 21:11:54 放弃了将所有哈希相关函数与结构体都从20字节转化为32字节的思路, 选择直接将哈希值长度改为32字节. 感觉除了需要内存多一点以外, 应该没什么副作用, 反正这个程序现在也需要兼容32字节的哈希, 如果使用20字节的哈希, 反正后12字节都是0, 不会影响正确性. 应该只需要改typedef fingerprint和config文件中的哈希长度. 如果需要兼容性再好一点 就把读写container的逻辑改一下 同时支持sha1和sha256的container. 

2024-3-9 15:27:12 解决了无法找到glib的问题. 发现glib已经在`/usr/local/lib`下. 解决方法: `export LDFLAGS="-L/usr/local/lib"`

2024-3-9 15:34:05 解决了make时报`/bin/sh: aclocal-1.14: command not found`的问题. 解决方法: `autoreconf -ivf`

2024-3-9 17:23:08 解决了`make install`时`/usr/local/bin`目录没有权限的问题, 解决方法: `./configure --prefix=/home/cbj/local/`指定安装目录

2024-3-9 17:51:34 成功运行了destor最基本的备份与恢复功能

2024-3-9 20:23:57 基本完成了recipe的设计. 做的更改较少, 主要是修改了jcr结构体, 添加了`new_id`和`new_bv`字段, 仿照写了`init_update_jcr`函数, 在bv, 即`backupVersion`结构体中, 有`metadata_fp`字段用来存储元数据的文件指针, 而id字段是备份序列号, 主要用于继承container, 只在rewrite节用到, 所以其实有没有都芜锁胃. 只需要在读取时使用原本的id与bv, 在写入recipe时使用新的id与bv即可. 为了实现这一目标, 在`destor.h`中添加了`IS_UPDATE_MODE`宏, 默认为0, 在`do_update`函数中将其设置为1, 在写入recipe的`filter_thread`函数中, 判断`IS_UPDATE_MODE`的值, 选择为函数传入不同的bv.

2024-3-10 11:25:36 研究了是否有必要设置新的`container_count`, 结果是没有必要, update时, 直接将其设置为0即可. container必须要两个, 不能直接进行覆盖写, 因为UPDATE时需要一边读一边写. 首先观察`container_count`的使用情况, 仅在开始结束, 创建新container以及`write_container`会被使用. `create_container`仅`filter_phase`使用, 再观察`write_container`, 仅被`filter_phase`和`append_thread`调用, 检查`append_thread`中的静态变量`container_buffer`, 仅在`retrieve_container_meta_by_id`这一函数中的用处值得注意, 这一函数在`do_delete`和`do_restore`中的`SIMULATION_RESTORE`场景被调用, 应该不是本项目的重点, 即使出问题也问题不大.

2024-3-10 12:50:08 基本完成了对containerstore的更改, 将之前的`IS_UPDATE_MODE`宏更改为直接用main函数的job, update时为`DESTOR_UPDATE`, 将fp区分为`old_fp`和`new_fp`, 将mutex区分为`old_mutex`和`new_mutex`, 只有在`do_update`时才会使用new系列的东西, 用来写入新的container, 所有的sha256 container都在`container.pool_new`, 与之前的进行区分. 在读取新的备份文件时, job为`DESTOR_NEW_RESTORE`, 仅更改读取的container的路径.

2024-3-10 16:27:28 解决了`containerstore.c`中`ser_end(cur, CONTAINER_META_SIZE);`assert failed的问题. 原因: 判断container是否溢出的`container_overflow`函数直接使用28指代了`sizeof(struct metaEntry)`, 由于更改了`fingerprint`的大小, 导致判断错误. 我也不知道他为什么非要写28, 他还特地在旁边备注`28 is the size of metaEntry.`.

2024-3-10 17:17:39 解决了运行原本的去重程序时报很多`Dedup phase: non-existing fingerprint`然后卡住的问题. 原因: 其实跟这个没关系, 主要是因为修改`containerstore.c retrieve_container_meta_by_id`函数时, `pthread_mutex_t *mutex`错误调用`pthread_mutex_lock(&mutex);`导致死锁.

2024-3-10 17:22:45 成功运行原本的backup与restore程序. 数据设计: ab两个文件均为随机10MB数据, c为a的复制. 运行backup和restore之后进行比较. 结果: 一致. 途中记录了日志, 在backup ab两个文件时, 大量有`Dedup phase: non-existing fingerprint`, 应该是正常现象, 在backup c时, 都是`Dedup phase: xxxth chunk is duplicate in container x`. 故认为程序运行无误.

2024-3-11 00:34:18 update后进行restore显示恢复文件数为0, 检查发现`open_backup_version`中读取的文件数就是0. 怀疑update写文件时出问题. 发现只有`append_file_recipe_meta`函数更改了backupVersion中的`number_of_files`. 排查各个`number_of_files`相关函数发现, 在`do_update`函数中没有执行`update_backup_version`和`free_backup_version`函数, 导致修改过的bv没有写入文件.

2024-3-11 01:11:37 已经实现第一阶段naive的初步版本, 可以将sha1备份转化为sha256备份并成功读出. 但目前的sha1备份全都是32字节哈希, 后12字节为全0, 后续可能需要实现20字节sha1备份的读取.

2024-3-11 21:12:29 研究了`index_lookup`函数, 发现有好几层的缓冲. 1. `storage_buffer.container_buffer`, 里面只有一个container, 是只有`filter_phase`用到的临时container缓冲区, 还没写满的那种. 2. `index_buffer.buffered_fingerprints`, 这个应该是我们通常认为的索引缓存, 在`index_lookup`的最后, 也把这个指纹写到了这个缓冲区. 3. `fingerprint_cache_lookup`, 它分成了物理局部性与逻辑局部性, 物理局部性最终指向`lru_cache_lookup`, 而这个LRU缓存仅在restore用到, 是读取chunk时的LRU缓存, 但`index_lookup`函数只在backup中用到了, 不知道他这里放这个是什么意思. 4. `kv_store`, 放在内存中的fp -> container id哈希表, 存了对应container的id.

2024-3-13 09:18:33 研究了`destor.index_key_size`成员, 发现这个其实更多的是用来设置索引的key大小, 在container读写的时候, 其实并没有用到这个, 准备采取其他的方法来实现20字节sha1的读写. 如果修改最少的东西, 可能就是把读写container的`sizeof(fingerprint)`和`container_overflow`都改一下, 思考了修改`struct metaEntry`的可能性, 感觉还是需要修改前面两个, 而且还得改一些`sizeof(struct metaEntry)`, 感觉更麻烦了.

2024-3-13 13:04:13 研究`retrieve_container_meta_by_id`函数的使用情况, 探究与`retrieve_container_by_id`的异同. `retrieve_container_meta_by_id`在`do_delete`和`do_restore`的`SIMULATION_RESTORE`使用, 这两个都无所谓, 重要的是`fingerprint_cache_prefetch`, 这个在`index_lookup_base`中使用, 对应的`index_lookup`在`dedup_phase`使用, 这个函数在backup和update都用到了. 在update时, 由于索引是新索引, 返回的metadata应该也是新container的, 所以应该使用`new_fp`.

2024-3-14 17:30:47 在尝试实现20字节backup时, 修复了报`NOTICE("Filter phase: A key collision occurs")`和100\%重复率的问题. 原因: 遍历container哈希表进行写入时, 在序列化时, 将指纹进行了memset, 由于遍历时使用的是指针, 可能导致了container中的指纹全部变成了0.

2024-3-14 20:02:08 研究了在shell脚本中使用destor时的配置文件路径问题, 使用两个不同的destor文件夹进行编译运行, 一个设置日志为debug, 一个设置为notice, 结果发现notice日志明显比debug小, 说明在shell脚本内执行destor是以脚本内的路径为准, 而非脚本开始执行时所在的路径.

2024-3-14 20:35:26 完成了20字节backup的工作, 并进行了测试, 使用原来和现在的两套系统同时分别进行一次备份与恢复, 备份内容相同, 最终从`container.pool`中读取的container数量也一致(均为0x66). 将二者的日志进行排序后diff, 除了速度不一致以外, 还有一些`Filter phase: write a segment start at offset xxxxx`不一致, 本来猜测和随机的分段有关, 但将两边都改成`uniform 64`还是不行.

2024-3-15 15:04:42 解决了update时重复率不为0.3333的问题. 原因: 数据读取线程`lru_get_chunk_thread`和`filter_phase`在`jcr.data_size`和`jcr.chunk_num++`的计数上有重复, 导致总数据量翻倍.

2024-3-15 15:47:48 研究update重复率为0和`retrieve_container_meta_by_id`应该采用哪个fp的问题, 在`index_lookup_base`中打印, 发现backup查重时, 会先在`key-value store`中找到并预取出来, 然后后面的就可以在`fingerprint cache`中找到了. 同时发现update时有`Filter phase: A key collision occurs`, 认为原因是`retrieve_container_meta_by_id`时取出错误.

2024-3-15 16:15:17 完成了20位哈希的实现. 只需要设置好`container_store.c`中的各个读写参数, 即可顺利完成读写. 测试方法与之前`2024-3-10 17:22:45`大致相同, 本次对输出的日志数据进行了进一步分析, 确保update和backup的重复率均为0.3333. 并进行了大文件测试, 测试了1.3G的centos备份文件, update与backup重复率均为0.1148.

2024-3-15 19:43:20 重新研究了一下index相关的代码, 对`2024-3-11 21:12:29`的研究提出了修改, 主要是`index_buffer.buffered_fingerprints`和`fingerprint_cache_lookup`, 前者是为了逻辑一致性设计的, 在实际使用中似乎并没有什么用, 但是还是会按顺序把代码都跑一遍, 后者使用的是已经封装解耦合比较完善的一个LRU, 进行了一定的包装, 这个才是我们认为的索引缓存. 每次container写满了之后, 会调用`index_update`函数, 这个实际上就是把container里面的所有东西都放到kvstore里面(为什么不插到LRU里面?). `fingerprint_cache`这边只有lookup和prefetch函数, 也就是说必须先读取kvstore, 才会把数据放到LRU里面, 不会有任何显式的LRU添加. 实际的实验也验证了这一点, 即使内容只有三个container, 并且LRU的存储单位是`struct containerMeta`, LRU的长度完全足够装下所有的container, 在重复数据出现时, 还是会触发prefetch.

2024-3-17 17:31:35 解决了添加`-i`参数后报`Segmentation fault`的问题. 原因: 在`destor.c`设置参数时, 设置为了`sr::n::u::t::p::h::i`, 导致i参数无法解析optarg.

2024-3-18 21:12:46 解决了添加了第二阶段索引之后, 大文件恢复错误的问题. 发现kvstore命中一次之后, 之后所有的LRU全部命中, 检查发现这次kvstore命中的20字节哈希为全0. 最终发现是`lru_get_chunk_thread`在读取container内容时换了一个chunk, 复制的时候还是使用`fp`字段, 导致之前放到`pre_fp`中的内容全部失效, 都变成了0.

2024-3-19 00:54:20 解决了新添加的索引没有用的问题. 发现`upgrade_kvstore_htable_lookup`函数中, 每次进入时, 哈希表都只有一个元素. 发现每次进入的时候, 指纹都是0, 尝试追溯发现`storage_buffer.chunks`里面的chunks也都是重新赋值的, 导致没有复制`old_fp`字段. 加上复制语句后程序成功运行.

2024-3-19 12:44:19 解决了LRU缓存不命中的问题. 原因: 在测试数据下, 重复数据都在kvstore中且不会第三次出现, 而LRU链表长度较小, 又是单条存储, 所以存储能力十分有限, 最终导致无法命中. 将LRU链表大小调大后即可.

2024-3-19 19:16:03 完成了第二阶段, 完善了相关的测试.

2024-3-21 17:35:27 解决了第三阶段哈希表可以插入, 在函数内也可以lookup到, 但函数外就查找不到的问题. 原因: 哈希表的键值对没有完全使用动态内存, key为`int64_t`, 并没有使用动态内存. 解决方法: malloc并copy一下.

2024-3-21 18:00:29 完成了第三阶段, 可以正确跑完runCorrect测试, 但内部细节还不明确, 不知道实现的对不对. 并且backup时会报`GLib-CRITICAL **: g_hash_table_destroy: assertion 'hash_table != NULL' failed`, 但暂时不影响结果.

2024-3-21 18:50:17 解决了backup时报destory空哈希表的问题. 原因: `init_kvstore`时根据 update level 选择不同的`init_upgrade_kvstore_htable`参数, 没有设置默认情况, 但close时依旧, 导致 backup close 时没有初始化哈希表.

2024-3-22 14:06:33 尝试将`index_lookup_base`中加入重复的判断条件, 如果重复则直接continue, 但是不太行, 因为`buffered fingerprints`查找部分需要对所有指纹建立`g_queue`, 如果跳过这段, 则会报很多`assertion 'queue != NULL' failed`.

2024-3-22 19:17:12 研究大文件update2时速度特慢, 并且可能直接Segmentation fault的问题. 段错误似乎是在`pre_dedup`时访问`storage_buffer.container_buffer`导致的, 它有时update1时就直接段错误, 有时在update2的一半时出现段错误, 所以估计是多线程问题. 对程序的时间进行测试, 发现update2速度慢是因为LRU访问时间过长, 比update1访问时间长很多, 虽然他俩的LRU长度是一样的.

2024-3-23 19:23:36 在进行大版本修改之前进行数据的记录, 方便后续对比, 记录在`log/prev_logs`

2024-3-25 14:40:40 修改了第二阶段的各阶段顺序, 把pre_dedup阶段和读数据阶段位置互换, 成功将container读取次数从1318降低到了444

2024-3-25 16:32:43 完成了TODO: 将upgrade_level的数字改为宏定义

2024-3-26 14:36:52 明确了第三阶段的各项具体实现细节. 1. chunks传递: 由于必须使用segmenting, 这里强制使用file-defined, 在FILE_START时添加segment, FILE_END时结束segment, 中间会有很多container传递过来, 在container内的chunk需要加入新的container中, 在container外的chunk是原本recipe中的一部分. 2. 索引查找与更新: 二维索引由于以containerid为key, 故不需要lru, 直接使用kvstore中的哈希表实现id到container的第一级映射, container也使用一个哈希表, 在CONTAINER_END时将container插入索引中.

2024-3-26 18:34:01 学习了如何进行debug. 并根据coredump文件进行了分析, 解决了第三阶段`Segmentation fault`的问题. 使用gdb调试发现错误位置在libcrypto.so, 判断是哈希阶段出现问题, 结果发现container外的chunk都没有数据, 应该直接传下去, 但是哈希阶段还是会对其进行处理, 导致出错.

2024-3-27 10:52:46 解决了第三阶段查询索引后id乱码的问题. 原因: 查询哈希表后得到的`upgrade_index_value_t`被错误认为是`upgrade_index_kv_t`.

2024-3-27 11:21:54 解决了第三阶段upgrade无法正常结束, 无法正常restore的问题. 原因: 2D的filter线程在结束前没有将status置为`JCR_STATUS_DONE`, 没有flush最后一个非空的container.

2024-4-1 15:15:10 完成了第三阶段的LRU和删除pre_dedup的工作, 并进行了简单的测试, 似乎没什么问题, 但是第二阶段没办法把LRU做成哈希表, 因为无法驱逐.

2024-4-1 20:21:08 解决了报错`double free`的问题, 原因: 在`filter_thread_2D`函数中, 将记录文件所有chunk的sequence进行了`g_sequence_free`, 并且设置了sequence的`free_func`, 但是在迭代sequence的时候, 又会调用`free_func`, 导致了`double free`.

2024-4-1 20:24:41 解决了第三阶段成功prefetch但是在缓存中仍然找不到的问题, 在小数据量测试中没问题, 大数据测试就有问题, 怀疑是LRU的驱逐策略有问题, 将LRU的大小设置为1成功在小数据复现, 进一步发现是由于LRU没有申请新的内存空间导致的问题. 解决方法: 为LRU中的`GHashTable *`建立一个

2024-4-3 15:08:28 完成了将第二阶段的LRU缓存升级为哈希+LRU. 跑evaluation发现有大量的`Writing a chunk already in the container buffer!`, 检查发现第二阶段的 filter phase 使用的是被简化的, 故换上原本的filter phase和分段策略, 问题解决. 同时发现naive版本的瓶颈主要是哈希, 读取chunk和dedup加起来和哈希也差不多. 但1D的pre_dedup时间仍然是绝对数量级上的大头. 2D的read_chunk占10s, 哈希占6s, filter占3s.

2024-4-3 15:15:25 分析1D的pre_dedup流程, 发现`upgrade_1D_fingerprint_cache_insert`这一函数是绝对的害人精, 继续分析

2024-4-3 17:42:13 研究了dedup阶段`index_buffer`的作用: 在index查找之后, 将当前的数据块加入`index_buffer`, 防止由于filter阶段插入索引表和dedup阶段查询索引表的时间差, 导致前后两个相同的数据块无法被正确去重.

2024-4-7 10:22:18 研究了1D的LRU耗时巨大的问题, 原因: 1D的LRU过长, 而glist查找最后一个元素的时间为O(n), 导致了巨大的时间消耗,见https://github.com/GNOME/glib/blob/main/glib/glist.c#L911. 原本希望直接首尾相接, 但是glist的实现很多都依赖首尾的两个空指针, 例如遍历search和获取首尾节点, 所以选择了在lruCache结构体中添加一个尾部指针并进行简单的维护即可.

2024-4-9 13:31:41 研究了各个phase线程的运行情况, 发现`lru_get_chunk`阶段基本上一直在运行, 而其他的线程都会统一在一段时间停一会, 据此认为程序为IO瓶颈.

2024-4-11 01:30:38 研究了mysql插入二进制数据后, 读取出来长度不一致的问题. 原因: 使用了char(32)来存储二进制数据, 可能导致截断与字符集问题. 解决方法: 改用`binary`类型, 在C语言中选用`MYSQL_TYPE_BLOB`类型即可.

2024-4-11 14:46:01 解决了执行sql语句时报`Commands out of sync; you can't run this command now`的问题, 原因: 执行完语句没有`mysql_stmt_free_result(stmt);`, 加上即可.

2024-4-11 19:57:25 研究了mysql运行速度过慢的问题, 在update过程中, 插入操作耗时总共372s, 共插入约45000个chunk, 为了判断是否是select操作有阻塞, 我首先使用了两个connect, 没有什么效果, 然后编译小型测试软件对其进行压力测试, 插入10000条一样大的数据共耗时84s, 与之前的速度差不多. 解决方法: 将预编译指令多搞几个value, 一次插入多个数据, 目前设定是10个.

2024-4-12 19:12:53 将1D和2D中间索引存储在mysql中

2024-4-12 19:16:14 研究了是否需要将kvstore中的数据存储到mysql中, 发现kvstore在restore时并不会用到, 只有在下一次bakcup时会用, 但是目前naive模式使用了这个, 所以还是需要进行修改.

2024-4-16 16:23:34 解决了`kvstore_mysql`运行时, read_container报错的问题, 原因: 初始化kvstore_mysql时, 使用了`mysql_real_query(&conn, KVSTORE_CLEAR_TABLE, strlen(KVSTORE_CLEAR_TABLE));`, 而conn变量是`static MYSQL *conn;`, 导致指针乱指.

2024-4-16 16:25:10 解决了`kvstore_mysql`运行时, 报`Lost connection to MySQL server during query`的问题, 初步判断是多线程访问同一个mysql连接导致的, 但是检索了kvstore的各处使用, 发现其实一直都会锁住`index_lock.mutex`, 反正在mysql这里加上锁就行, 后续再考虑怎么去掉锁.

2024-4-16 20:28:38 解决了2D在第128个container时会写入sql之后无法找到的问题. 问题原因: 我也搞不清楚. 解决方法: 在将container插入mysql的同时, 把2D的放到LRU中.

2024-4-17 02:09:00 解决了2D将键值对重复插入kvstore的问题, 原因: `upgrade_fingerprint_cache_prefetch`没有设置返回值, 导致程序误以为没有取回数据. 同时修复了由于`upgrade_index_lock`定义不同导致的结构体混乱问题, 但是好像由于只用到了结构体第一部分的mutex锁, 并没有造成实际影响.

2024-5-27 00:00:30 尝试解决1D加入SQL缓存后, restore报`expect 140342960128160, but read 0; destor: containerstore.c:303: retrieve_container_by_id: Assertion c->meta.id == id' failed.`的问题, 看报错是写入文件的recipe出问题了. 如果开启内存检查, 则会在`insert_sql_store_buffered_1D`函数执行SQL查询的时候就报读取0地址的错误, 目前还搞不清楚为什么. 最后发现是调用`insert_sql_store_buffered_1D`时, 原本应传`upgrade_index_value_t*`传成了`upgrade_index_value_t**`.

-static-libasan -fno-omit-frame-pointer -fno-stack-protector -fsanitize=address -fsanitize=undefined -fsanitize=leak -fsanitize-recover=all
