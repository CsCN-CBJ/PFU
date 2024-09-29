#include "destor.h"
#include "jcr.h"
#include "recipe/recipestore.h"
#include "storage/containerstore.h"
#include "utils/lru_cache.h"
#include "update.h"
#include "backup.h"
#include "index/index.h"

/* defined in index.c */
extern struct index_overhead index_overhead, upgrade_index_overhead;
extern GHashTable *upgrade_processing;
extern GHashTable *upgrade_container;

upgrade_lock_t upgrade_index_lock;

static void* read_recipe_thread(void *arg) {

	int i, j, k;
	fingerprint zero_fp;
	memset(zero_fp, 0, sizeof(fingerprint));
	DECLARE_TIME_RECORDER("read_recipe_thread");
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		BEGIN_TIME_RECORD;
		struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);
		END_TIME_RECORD;
		NOTICE("Send recipe %s", r->filename);

		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		SET_CHUNK(c, CHUNK_FILE_START);

		TIMER_END(1, jcr.read_recipe_time);

		sync_queue_push(upgrade_recipe_queue, c);

		for (j = 0; j < r->chunknum; j++) {
			TIMER_DECLARE(1);
			TIMER_BEGIN(1);

			BEGIN_TIME_RECORD
			struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, 1, &k);
			END_TIME_RECORD

			struct chunk* c = new_chunk(0);
			memcpy(&c->old_fp, &cp->fp, sizeof(fingerprint));
			assert(!memcmp(c->old_fp + 20, zero_fp, 12));
			c->size = cp->size;
			c->id = cp->id;

			TIMER_END(1, jcr.read_recipe_time);

			sync_queue_push(upgrade_recipe_queue, c);
			free(cp);
		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(upgrade_recipe_queue, c);

		free_file_recipe_meta(r);
	}

	FINISH_TIME_RECORD
	sync_queue_term(upgrade_recipe_queue);
	return NULL;
}

uint64_t LT_k[] = {
    0x8c966374151a67b6, 0x3edf7efe33cdfd7b, 0x5a2c4a1a310be558,
    0x69e43fcb628fbeb7, 0x99bd77ef54c90d36, 0xbab1c4f59f066e65,
    0x20420bdd061f48c4, 0xb0e0c20838c35c57, 0xeddd78004259c3d2,
    0xf298ed021e76023a, 0xb9e8b29602ced7f3, 0x853af80c4f919742,
    0x505b7d96bf01b253, 0xa318b2bff19ed50c, 0x5308029ce76f358f,
    0x9a398f26b33f24a,  0x74595977a450ca7b, 0x4e8468ce85680390,
    0x2436fba706b7bf67, 0xfc0923f5563e8424, 0x5fcbfacf4f88506b,
    0xb7b854be9806aa3b, 0xa2256b1fc3bd00fe, 0x3ea3e4597d23917d,
    0xd952cb1a7656852e, 0x89e052b4e095f921, 0x4be913d1c4f79a82,
    0x9d36a507aa427257, 0x69b7818b57dbbf8,  0x21102213d2cae2cd,
    0x27ec5888afbdb47e, 0x4f16a4e8c63e27f9, 0x250a3857af744546,
    0x3e97bbeec0fcabdd, 0x4b6c65f8ca7fd632, 0xa91fce0f23e1be61,
    0x5050c5a688052206, 0x9e2e3f1a6b85328f, 0xe09ec18b956f5fea,
    0x4000f41b1b5b25e7, 0x3fbea3faba569084, 0xe471347f40647f65,
    0xda7abaa5f3caf2c8, 0x189201a4940001f9, 0x29e183e37d7da328,
    0xd2ae28418f093e67, 0x8e5cb797039be80e, 0x41604277260a071d,
    0x797fd05581e14764, 0x75de5603975e3ea9, 0x11ef41165111c73c,
    0xb7ddf0821a4031f5, 0x3eec9e62cf45d24c, 0xcda86289d51cd2c7,
    0x127483edee743fae, 0x4c62baba754703cf, 0xcd8d463bb2e9583a,
    0xaa38a80cc6c609f3, 0x5f97356ca50a5986, 0x220a1b6bc88caeb,
    0x11edac1b01696b0e, 0x6e27fc4394cebee3, 0xeeffd1b2b382a2c0,
    0x2443385d4c4e195,  0x2ee724f1ad94f68e, 0xa4491e380c5cf7b,
    0xfe36dbfaed334ef3, 0xdd1b785765b2bb6,  0xcc3461a160502f47,
    0x64b1c122c7083b1a, 0x388e6a418b6fd359, 0x65809585f16ab490,
    0x3eb1ec4b9577e4c7, 0xd80e411261617dd8, 0x7da08fd8df71e169,
    0xa20af93edb933a86, 0x59fb1b86e1ff5415, 0x5b596e7a23e7af9a,
    0x50a905ffc0a402c3, 0x3a8cefabefd9dbbe, 0xc9762597ccab4a09,
    0xd8de3774872f3efe, 0xc9f1075d9ebc3c51, 0x3b55ac8bfcf8f51e,
    0xe4e88fb0fa555bd1, 0xfa725ab5e019900a, 0x4be7597f3fcb499d,
    0xe108b91410eb4788, 0xb9f7fc4896cbf4c3, 0x8cdaddac2fc852ee,
    0x5c1d3439307f3d03, 0x60e5ae5e93c82d50, 0x340d48d6be8c5b09,
    0x9a02c3be5b6c05e8, 0x56014afc31084d83, 0x85b888f604ea56f2,
    0x468f4da9d12660f7, 0x9005e918384c13d0, 0x29ba77f861e74697,
    0x8237cf1734b2c668, 0xee06f9f1df6ced7b, 0x142936b9399add6a,
    0x241ad24ae452fe07, 0x942b4c3c79dd1dbe, 0x99b14f06198c22a7,
    0xe2987fcf99376312, 0x844daa9a945c9067, 0xfa234ecf470184dc,
    0x6d97ccd39c1eb593, 0x8bf12e40249118d0, 0x923f72bcdb934d2f,
    0x69bd5c907fc76c8e, 0xe5827868949fc4f3, 0x4f5d13c3d6e20e0a,
    0xb3646a9d0111233,  0xf0af68353b17c0c8, 0xc920f56447f90673,
    0x8960c8168641fe16, 0xfee91d454a19219,  0xb25b446e135cf6b2,
    0x761b04f2362314f9, 0xa151afe2fb1ff5be, 0x2e973d6af5de4037,
    0x485c4501258ab54c, 0xf21bd1e05d869951, 0xd79097aaa1050314,
    0x2b5e8c12e04ff4f9, 0x4e43a881e78d9764, 0x16d02eca685abdab,
    0x7913757d06ccfaec, 0x513242305e9af1ef, 0xc847965583801b62,
    0x8862452b0de8c5e5, 0xce5ae051740dea5a, 0x1a028ca4bb2875a3,
    0x5680bba4aad7ffa6, 0x324d2adfb43a331b, 0xe456b7b1c0301b68,
    0x7801a00c795d859d, 0x41bdd48db6ae14a,  0x5fa8107e14c841fb,
    0xd0e4bdea28bef85c, 0x77bdb5eb30614b89, 0xdefa9fd302bbd858,
    0x8a54bfa54688dad,  0x682ec11a915f0980, 0xc4af0b1c0ffec719,
    0xf76fd41604e89104, 0xeb01bf3d9ced6817, 0xa87180c091474c6e,
    0x351bdb3557277969, 0xee81b4ce09b723aa, 0x3bd353a36b05adb,
    0x7bcb44892055af78, 0xadfb4e960a0ca951, 0x2273bad9b4ac3d74,
    0xebb4454444aaa94b, 0xe686846acad641a8, 0x6a6c096404d1c7a3,
    0x3d857f91fc5f6232, 0x5c63557a89fa3a27, 0x3ee7bc50de6d3e04,
    0x2490253782ef8a57, 0xad4d59ff8fa5f4c,  0x1c01a62b4c726533,
    0x9ef66ba35cb5ab2e, 0x4c47d62317d71cdf, 0x2a40c557d5a3f2a0,
    0xf758338d53442abd, 0x1eaefcd073bcdd10, 0xb9833e0eb50e8fd7,
    0xcb3c0a27878d6de5, 0x49b827acbe6d77ac, 0x65903aad1d6c1e9f,
    0x7a7c66f221cf20ca, 0x9a0f5565415a795,  0xbe5a882391e5527e,
    0x14d2a06077f8339d, 0xcad2bce34bf19652, 0x5541cc588464c621,
    0xc44d981a4aa21e70, 0xb5f4dbb200e75029, 0xa0fc23f2eef70334,
    0xd5e94a4243d9b506, 0xc6dc1d3da6a23d23, 0xf3b4295d0dd364f2,
    0x57750173991256b5, 0x119097313522fa6a, 0xbeecd02c90273c43,
    0x5cb1a8a10079cb23, 0xc52b57c63ff9a0ca, 0xf35659fc64e0143,
    0xbdf325035c594f38, 0xd1f8101a35320afd, 0xe899458626d703f8,
    0xbf97f530ac049837, 0x895021a6620ae70,  0x948596cd24280401,
    0x471f6accca7627f4, 0x4a129ff1164598bd, 0x71405c568bef229e,
    0x4fda2755c3737887, 0x909a3cc70ee44b32, 0xd1a8a3c3dc9fd44d,
    0x5ebb36a043ede1ca, 0xf6a89a10d5e68b53, 0xd9bf7a1aaa5016b2,
    0x71030478353d66cb, 0x6806d8ba045b5ae0, 0xa7672b4808466f67,
    0xf93e0da0d5011f0a, 0xa8aeeeec63465029, 0xe958e539a532a76e,
    0xfd8bf78ab9628da1, 0xca1eb1fe8a769e6a, 0x2a96875d6c8f9257,
    0x501c523d559d7d12, 0x9b7e72aa3d7f1a65, 0x25554c1d398077a,
    0xcdd8af6cd9c471e1, 0x39bea30bd9a49e,   0x233b737e0eeee721,
    0xd6d57c6896b14ed4, 0x8dabe80e8c70e265, 0x1b859ed8c291ddbc,
    0x6b3385f41e0a598b, 0xcf05250d14390e94, 0xc58d5e7d9c8b9f73,
    0x49a5c7a9ba27febc, 0x4841a129f0804005, 0xe7ffa313ce3144a,
    0x13d8768f158b32cb, 0x5d4a2c0fe9cd7afe, 0xe5d0ae6ab568df33,
    0xdc377e207d3c5d43, 0x626790b237a4ab52, 0xfad9bf3a472cfe4d,
    0xa2a6bc5395bbce52, 0xce0a8e4ef2f3ee3f, 0xb4b5b36cf31b4d66,
    0xef69efdc30e9077e, 0xcf28f3bbba364c83, 0xc8b80c742bfdd966,
    0x83f12924c9400e15, 0xa35b3222d11d583e, 0x9c9a9d426cde5fdd,
    0xbe722684e90680f2, 0x1119ab5f71bd737d, 0xc739c71894e34ba8,
    0xcd822b0cb4e2e159, 0x67b583610a0410e0, 0xae18b6eb024bdfdb,
    0x865951f3e76f5834, 0x18eebfb065ebcb59, 0xd35f0999dd5e5b00,
    0xeb48a728aaf18d2e,
};

uint64_t LT_b[] = {
    0xcdd8af6cd9c471e1, 0x39bea30bd9a49e,   0x233b737e0eeee721,
    0xd6d57c6896b14ed4, 0x8dabe80e8c70e265, 0x1b859ed8c291ddbc,
    0x6b3385f41e0a598b, 0xcf05250d14390e94, 0xc58d5e7d9c8b9f73,
    0x49a5c7a9ba27febc, 0x4841a129f0804005, 0xe7ffa313ce3144a,
    0x13d8768f158b32cb, 0x5d4a2c0fe9cd7afe, 0xe5d0ae6ab568df33,
    0xdc377e207d3c5d43, 0x626790b237a4ab52, 0xfad9bf3a472cfe4d,
    0xd952cb1a7656852e, 0x89e052b4e095f921, 0x4be913d1c4f79a82,
    0x9d36a507aa427257, 0x69b7818b57dbbf8,  0x21102213d2cae2cd,
    0x27ec5888afbdb47e, 0x4f16a4e8c63e27f9, 0x250a3857af744546,
    0x3e97bbeec0fcabdd, 0x4b6c65f8ca7fd632, 0xa91fce0f23e1be61,
    0x5050c5a688052206, 0x9e2e3f1a6b85328f, 0xe09ec18b956f5fea,
    0x4000f41b1b5b25e7, 0x3fbea3faba569084, 0xe471347f40647f65,
    0xda7abaa5f3caf2c8, 0x189201a4940001f9, 0x29e183e37d7da328,
    0xd2ae28418f093e67, 0x8e5cb797039be80e, 0x41604277260a071d,
    0x797fd05581e14764, 0x75de5603975e3ea9, 0x11ef41165111c73c,
    0xb7ddf0821a4031f5, 0x3eec9e62cf45d24c, 0xcda86289d51cd2c7,
    0x127483edee743fae, 0x4c62baba754703cf, 0xcd8d463bb2e9583a,
    0xaa38a80cc6c609f3, 0x5f97356ca50a5986, 0x220a1b6bc88caeb,
    0x11edac1b01696b0e, 0x6e27fc4394cebee3, 0xeeffd1b2b382a2c0,
    0x2443385d4c4e195,  0x2ee724f1ad94f68e, 0xa4491e380c5cf7b,
    0xfe36dbfaed334ef3, 0xdd1b785765b2bb6,  0xcc3461a160502f47,
    0x64b1c122c7083b1a, 0x388e6a418b6fd359, 0x65809585f16ab490,
    0x3eb1ec4b9577e4c7, 0xd80e411261617dd8, 0x7da08fd8df71e169,
    0xa20af93edb933a86, 0x59fb1b86e1ff5415, 0x5b596e7a23e7af9a,
    0x50a905ffc0a402c3, 0x3a8cefabefd9dbbe, 0xc9762597ccab4a09,
    0xd8de3774872f3efe, 0xc9f1075d9ebc3c51, 0x3b55ac8bfcf8f51e,
    0xe4e88fb0fa555bd1, 0xfa725ab5e019900a, 0x4be7597f3fcb499d,
    0xe108b91410eb4788, 0xb9f7fc4896cbf4c3, 0x8cdaddac2fc852ee,
    0x5c1d3439307f3d03, 0x60e5ae5e93c82d50, 0x340d48d6be8c5b09,
    0x9a02c3be5b6c05e8, 0x56014afc31084d83, 0x85b888f604ea56f2,
    0x468f4da9d12660f7, 0x9005e918384c13d0, 0x29ba77f861e74697,
    0x8237cf1734b2c668, 0xee06f9f1df6ced7b, 0x142936b9399add6a,
    0x241ad24ae452fe07, 0x942b4c3c79dd1dbe, 0x99b14f06198c22a7,
    0xe2987fcf99376312, 0x844daa9a945c9067, 0xfa234ecf470184dc,
    0x6d97ccd39c1eb593, 0x8bf12e40249118d0, 0x923f72bcdb934d2f,
    0x69bd5c907fc76c8e, 0xe5827868949fc4f3, 0x4f5d13c3d6e20e0a,
    0xb3646a9d0111233,  0xf0af68353b17c0c8, 0xc920f56447f90673,
    0x8960c8168641fe16, 0xfee91d454a19219,  0xb25b446e135cf6b2,
    0x761b04f2362314f9, 0xa151afe2fb1ff5be, 0x2e973d6af5de4037,
    0x485c4501258ab54c, 0xf21bd1e05d869951, 0xd79097aaa1050314,
    0x8c966374151a67b6, 0x3edf7efe33cdfd7b, 0x5a2c4a1a310be558,
    0x69e43fcb628fbeb7, 0x99bd77ef54c90d36, 0xbab1c4f59f066e65,
    0x20420bdd061f48c4, 0xb0e0c20838c35c57, 0xeddd78004259c3d2,
    0xf298ed021e76023a, 0xb9e8b29602ced7f3, 0x853af80c4f919742,
    0x505b7d96bf01b253, 0xa318b2bff19ed50c, 0x5308029ce76f358f,
    0x9a398f26b33f24a,  0x74595977a450ca7b, 0x4e8468ce85680390,
    0x2436fba706b7bf67, 0xfc0923f5563e8424, 0x5fcbfacf4f88506b,
    0xb7b854be9806aa3b, 0xa2256b1fc3bd00fe, 0x3ea3e4597d23917d,
    0x2b5e8c12e04ff4f9, 0x4e43a881e78d9764, 0x16d02eca685abdab,
    0x7913757d06ccfaec, 0x513242305e9af1ef, 0xc847965583801b62,
    0x8862452b0de8c5e5, 0xce5ae051740dea5a, 0x1a028ca4bb2875a3,
    0x5680bba4aad7ffa6, 0x324d2adfb43a331b, 0xe456b7b1c0301b68,
    0x7801a00c795d859d, 0x41bdd48db6ae14a,  0x5fa8107e14c841fb,
    0xd0e4bdea28bef85c, 0x77bdb5eb30614b89, 0xdefa9fd302bbd858,
    0x8a54bfa54688dad,  0x682ec11a915f0980, 0xc4af0b1c0ffec719,
    0xf76fd41604e89104, 0xeb01bf3d9ced6817, 0xa87180c091474c6e,
    0x351bdb3557277969, 0xee81b4ce09b723aa, 0x3bd353a36b05adb,
    0x7bcb44892055af78, 0xadfb4e960a0ca951, 0x2273bad9b4ac3d74,
    0xebb4454444aaa94b, 0xe686846acad641a8, 0x6a6c096404d1c7a3,
    0x3d857f91fc5f6232, 0x5c63557a89fa3a27, 0x3ee7bc50de6d3e04,
    0x2490253782ef8a57, 0xad4d59ff8fa5f4c,  0x1c01a62b4c726533,
    0x9ef66ba35cb5ab2e, 0x4c47d62317d71cdf, 0x2a40c557d5a3f2a0,
    0xf758338d53442abd, 0x1eaefcd073bcdd10, 0xb9833e0eb50e8fd7,
    0xcb3c0a27878d6de5, 0x49b827acbe6d77ac, 0x65903aad1d6c1e9f,
    0x7a7c66f221cf20ca, 0x9a0f5565415a795,  0xbe5a882391e5527e,
    0x14d2a06077f8339d, 0xcad2bce34bf19652, 0x5541cc588464c621,
    0xc44d981a4aa21e70, 0xb5f4dbb200e75029, 0xa0fc23f2eef70334,
    0xd5e94a4243d9b506, 0xc6dc1d3da6a23d23, 0xf3b4295d0dd364f2,
    0x57750173991256b5, 0x119097313522fa6a, 0xbeecd02c90273c43,
    0x5cb1a8a10079cb23, 0xc52b57c63ff9a0ca, 0xf35659fc64e0143,
    0xbdf325035c594f38, 0xd1f8101a35320afd, 0xe899458626d703f8,
    0xbf97f530ac049837, 0x895021a6620ae70,  0x948596cd24280401,
    0x471f6accca7627f4, 0x4a129ff1164598bd, 0x71405c568bef229e,
    0x4fda2755c3737887, 0x909a3cc70ee44b32, 0xd1a8a3c3dc9fd44d,
    0x5ebb36a043ede1ca, 0xf6a89a10d5e68b53, 0xd9bf7a1aaa5016b2,
    0x71030478353d66cb, 0x6806d8ba045b5ae0, 0xa7672b4808466f67,
    0xf93e0da0d5011f0a, 0xa8aeeeec63465029, 0xe958e539a532a76e,
    0xfd8bf78ab9628da1, 0xca1eb1fe8a769e6a, 0x2a96875d6c8f9257,
    0x501c523d559d7d12, 0x9b7e72aa3d7f1a65, 0x25554c1d398077a,
    0xa2a6bc5395bbce52, 0xce0a8e4ef2f3ee3f, 0xb4b5b36cf31b4d66,
    0xef69efdc30e9077e, 0xcf28f3bbba364c83, 0xc8b80c742bfdd966,
    0x83f12924c9400e15, 0xa35b3222d11d583e, 0x9c9a9d426cde5fdd,
    0xbe722684e90680f2, 0x1119ab5f71bd737d, 0xc739c71894e34ba8,
    0xcd822b0cb4e2e159, 0x67b583610a0410e0, 0xae18b6eb024bdfdb,
    0x865951f3e76f5834, 0x18eebfb065ebcb59, 0xd35f0999dd5e5b00,
    0xeb48a728aaf18d2e,
};
#define FEATURE_NUM 4
typedef uint64_t feature;
#define CALC_FEATURE(x, k) (((feature)(x)) * LT_k[k] + LT_b[k])

struct featureList {
	feature feature;
	size_t count;
	size_t max_count;
	containerid *recipeIDList;
};

void free_featureList(gpointer data) {
	struct featureList *list = data;
	free(list->recipeIDList);
	free(list);
}

int compare_container_id(void *a, void *b) {
	return *(containerid*) a == *(containerid*) b;
}

typedef struct recipeUnit {
	int merge_flag; // 0: sub, 1: merge
	struct fileRecipeMeta *recipe;
	struct chunkPointer *chunks;

	containerid sub_id;
	containerid total_num;
	struct recipeUnit *next;
} recipeUnit_t;

static int process_recipe(recipeUnit_t ***recipeList, GHashTable *featureTable[FEATURE_NUM]) {
	int i, j, k;
	int size = 0, max_size = 8;
	recipeUnit_t **list = malloc(sizeof(recipeUnit_t *) * max_size);
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		feature features[4] = { LLONG_MAX, LLONG_MAX, LLONG_MAX, LLONG_MAX };
		struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);
		struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, r->chunknum, &k);
		assert(r->chunknum == k);

		recipeUnit_t *unit = malloc(sizeof(recipeUnit_t));
		unit->merge_flag = 1;
		unit->recipe = r;
		unit->chunks = cp;
		unit->next = NULL;
		if (size >= max_size) {
			max_size *= 2;
			list = realloc(list, sizeof(recipeUnit_t) * max_size);
		}
		list[size] = unit;

		// calculate features
		for (j = 0; j < r->chunknum; j++) {
			for (k = 0; k < FEATURE_NUM; k++) {
				features[k] = MIN(features[k], CALC_FEATURE(cp[j].id, k));
			}
		}
		// insert features into featureTable separately
		for (k = 0; k < FEATURE_NUM; k++) {
			struct featureList *list = g_hash_table_lookup(featureTable[k], &features[k]);
			if (!list) {
				list = malloc(sizeof(struct featureList));
				list->count = 0;
				list->max_count = 1;
				list->recipeIDList = malloc(sizeof(containerid) * list->max_count);
				list->feature = features[k];
				g_hash_table_insert(featureTable[k], &list->feature, list);
			} else if (list->count >= list->max_count) {
				list->max_count *= 2;
				list->recipeIDList = realloc(list->recipeIDList, sizeof(containerid) * list->max_count);
			}
			list->recipeIDList[list->count++] = size;
		}
		jcr.pre_process_file_num++;
		size++;
	}
	*recipeList = list;
	return size;
}

static void* read_similarity_recipe_thread(void *arg) {

	int i, j, k;
	fingerprint zero_fp;
	memset(zero_fp, 0, sizeof(fingerprint));
	DECLARE_TIME_RECORDER("read_recipe_thread");
	recipeUnit_t **recipeList;
	// list [ hashtable [ feature -> featureList[ recipe id ] ] ]
	GHashTable *featureTable[FEATURE_NUM];
	for (i = 0; i < FEATURE_NUM; i++) {
		featureTable[i] = g_hash_table_new_full(g_int64_hash, g_int64_equal, free_featureList, NULL);
	}

	// read all recipes and calculate features
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	BEGIN_TIME_RECORD;
	int recipe_num = process_recipe(&recipeList, featureTable);
	TIMER_END(1, jcr.read_recipe_time);
	END_TIME_RECORD;

	// send recipes
	struct lruCache *lru = new_lru_cache(destor.index_cache_size, free, compare_container_id);
	feature featuresInLRU[FEATURE_NUM] = { LLONG_MAX, LLONG_MAX, LLONG_MAX, LLONG_MAX };
	GHashTable *sendedRecipe = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, NULL);
	for (i = 0; i < jcr.bv->number_of_files; i++) {
		// 选择一个与当前缓存最相似的recipe
		// 使用新的htb记录recipe的引用次数
		// containerid recipeID -> int64_t ref
		GHashTable *recipeRef = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, free);
		containerid bestRecipeID = -1;
		int64_t bestRecipeRef = -1;
		for (j = 0; j < FEATURE_NUM && bestRecipeRef < FEATURE_NUM && i != 0; j++) {
			feature f = featuresInLRU[j];
			assert(f != LLONG_MAX);
			struct featureList *list = g_hash_table_lookup(featureTable[j], &f);
			if (!list) continue;
			assert(list->feature == f);
			for (k = 0; k < list->count; k++) {
				containerid rid = list->recipeIDList[k];
				// 跳过已发送的recipe
				if (g_hash_table_lookup(sendedRecipe, &rid)) continue;
				// recipe引用次数+1
				int64_t *ref_p = (int64_t*)g_hash_table_lookup(recipeRef, &rid);
				if (ref_p) {
					(*ref_p)++;
				} else {
					containerid *id_p = malloc(sizeof(containerid));
					*id_p = rid;
					ref_p = malloc(sizeof(int64_t));
					*ref_p = 1;
					g_hash_table_insert(recipeRef, id_p, ref_p);
				}
				// 更新最佳recipe
				if (*ref_p > bestRecipeRef) {
					bestRecipeRef = *ref_p;
					bestRecipeID = rid;
					assert(bestRecipeRef <= FEATURE_NUM);
					if (bestRecipeRef == FEATURE_NUM) break;
				}
			}
		}
		g_hash_table_destroy(recipeRef);
		// 如果没有找到任何相似的recipe, 选择第一个未发送的recipe
		if (bestRecipeID == -1) {
			for (containerid id = 0; id < jcr.bv->number_of_files; id++) {
				if (!g_hash_table_lookup(sendedRecipe, &id)) {
					bestRecipeID = id;
					break;
				}
			}
		}
		assert(bestRecipeID >= 0);

		// 使用htb标记recipe是否已经发送
		containerid *recipeID_p = malloc(sizeof(containerid));
		*recipeID_p = bestRecipeID;
		recipeUnit_t *unit = recipeList[bestRecipeID];
		struct fileRecipeMeta *r = unit->recipe;
		struct chunkPointer *cp = unit->chunks;
		WARNING("Send recipe %s", r->filename);
		g_hash_table_insert(sendedRecipe, recipeID_p, "1");

		// 发送recipe
		struct chunk *c = new_chunk(sdslen(r->filename) + 1);
		strcpy(c->data, r->filename);
		SET_CHUNK(c, CHUNK_FILE_START);
		sync_queue_push(upgrade_recipe_queue, c);

		for (j = 0; j < r->chunknum; j++) {
			// 遍历recipe中所有chunk, 对其containerid进行特征计算
			struct chunk* c = new_chunk(0);
			memcpy(&c->old_fp, &cp[j].fp, sizeof(fingerprint));
			assert(!memcmp(c->old_fp + 20, zero_fp, 12));
			c->size = cp[j].size;
			c->id = cp[j].id;
			sync_queue_push(upgrade_recipe_queue, c);

			// simulate LRU
			// 如果找到, 会自动将其移到头部
			containerid *id_p = lru_cache_lookup(lru, &c->id);
			if (id_p) continue;
			// 插入新的containerid
			// 如果在container hashtable中找到, 实际上对于LRU来说和一个新的container是一样的, 不需要额外处理
			id_p = malloc(sizeof(containerid));
			*id_p = c->id;
			for (k = 0; k < FEATURE_NUM; k++) {
				feature f = CALC_FEATURE(c->id, k);
				if (f == featuresInLRU[k]) {
					WARNING("Feature conflict %lld %d %lx %lx", c->id, k, f, featuresInLRU[k]);
					fprintf(stderr, "Feature conflict %lld %d %lx %lx\n", c->id, k, f, featuresInLRU[k]);
				}
				if (f <= featuresInLRU[k]) {
					featuresInLRU[k] = f;
				} else if (lru_cache_is_full(lru) && CALC_FEATURE(*(containerid *)lru->elem_queue_tail->data, k) == featuresInLRU[k]) {
					// 如果最小值是最后一个, 需要重新计算特征
					feature best = f;
					GList* elem = g_list_first(lru->elem_queue);
					// 最后一个不参与
					while (elem && elem->next) {
						best = MIN(best, CALC_FEATURE(*(containerid *)elem->data, k));
						elem = g_list_next(elem);
					}
					featuresInLRU[k] = best;
				}
			}
			lru_cache_insert(lru, id_p, NULL, NULL);
		}

		c = new_chunk(0);
		SET_CHUNK(c, CHUNK_FILE_END);
		sync_queue_push(upgrade_recipe_queue, c);

		free_file_recipe_meta(r);
		free(cp);
		free(unit);
	}

	FINISH_TIME_RECORD
	sync_queue_term(upgrade_recipe_queue);
	free_lru_cache(lru);
	g_hash_table_destroy(sendedRecipe);
	for (i = 0; i < FEATURE_NUM; i++) {
		g_hash_table_destroy(featureTable[i]);
	}
	free(recipeList);
	return NULL;
}

static void* lru_get_chunk_thread(void *arg) {
	struct lruCache *cache;
	// if (destor.simulation_level >= SIMULATION_RESTORE)
	cache = new_lru_cache(destor.restore_cache[1], free_container,
			lookup_fingerprint_in_container);

	DECLARE_TIME_RECORDER("lru_get_chunk_thread");
	struct chunk* c;
	while ((c = sync_queue_pop(pre_dedup_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END) || CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			sync_queue_push(upgrade_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		// if (destor.simulation_level >= SIMULATION_RESTORE) {
		struct container *con = lru_cache_lookup(cache, &c->old_fp);
		if (!con) {
			BEGIN_TIME_RECORD;
			con = retrieve_container_by_id(c->id);
			END_TIME_RECORD;
			lru_cache_insert(cache, con, NULL, NULL);
			jcr.read_container_num++;
		}
		struct chunk *rc = get_chunk_in_container(con, &c->old_fp);
		memcpy(rc->old_fp, c->old_fp, sizeof(fingerprint));
		rc->id = TEMPORARY_ID;
		assert(rc);
		TIMER_END(1, jcr.read_chunk_time);

		sync_queue_push(upgrade_chunk_queue, rc);

		// filter_phase已经算过一遍了
		// jcr.data_size += c->size;
		// jcr.chunk_num++;
		free_chunk(c);
	}
	FINISH_TIME_RECORD;

	sync_queue_term(upgrade_chunk_queue);

	free_lru_cache(cache);

	return NULL;
}

static void* lru_get_chunk_thread_2D(void *arg) {
	DECLARE_TIME_RECORDER("lru_get_chunk_thread");
	struct chunk *c, *ck; // c: get from queue, ck: temp chunk
	struct container *con_buffer = NULL; // 防止两个相邻的container读两次
	while ((c = sync_queue_pop(pre_dedup_queue))) {

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END) || CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			sync_queue_push(upgrade_chunk_queue, c);
			continue;
		}

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);

		// 已经发送过的container不再发送
		assert(c->id >= 0);
		DEBUG("lru_get_chunk_thread_2D %ld", c->id);
		BEGIN_TIME_RECORD;
		pthread_mutex_lock(&upgrade_index_lock.mutex);
		struct containerMap *cm = g_hash_table_lookup(upgrade_container, &c->id);
		pthread_mutex_unlock(&upgrade_index_lock.mutex);
		struct container **conList;
		int16_t container_num = 0;
		int is_new;
		// 将需要读取的container放到conList中
		if (cm) {
			assert(cm->old_id == c->id);
			conList = malloc(sizeof(struct container *) * cm->container_num);
			DEBUG("Read new container %ld %ld %d", c->id, cm->new_id, cm->container_num);
			for (int i = 0; i < cm->container_num; i++) {
				if (con_buffer && con_buffer->meta.id == cm->new_id + i) {
					conList[i] = con_buffer;
					con_buffer = NULL;
					jcr.read_container_new_buffered++;
				} else {
					conList[i] = retrieve_new_container_by_id(cm->new_id + i);
					jcr.read_container_new++;
				}
			}
			container_num = cm->container_num;
			is_new = TRUE;
		} else {
			conList = malloc(sizeof(struct container *));
			conList[0] = retrieve_container_by_id(c->id);
			jcr.read_container_num++;
			assert(conList[0]);
			container_num = 1;
			is_new = FALSE;
		}
		END_TIME_RECORD

		// send container
		ck = new_chunk(0);
		SET_CHUNK(ck, CHUNK_CONTAINER_START);
		if (is_new) {
			SET_CHUNK(ck, CHUNK_REPROCESS);
		}
		TIMER_END(1, jcr.read_chunk_time);
		sync_queue_push(upgrade_chunk_queue, ck);
		TIMER_BEGIN(1);

		GHashTableIter iter;
		gpointer key, value;
		for (int i = 0; i < container_num; i++) {
			struct container *con = conList[i];
			g_hash_table_iter_init(&iter, con->meta.map);
			while(g_hash_table_iter_next(&iter, &key, &value)){
				ck = get_chunk_in_container(con, key);
				assert(ck);
				if (is_new) {
					ck->id = con->meta.id;
					SET_CHUNK(ck, CHUNK_REPROCESS);
				} else {
					memcpy(ck->old_fp, ck->fp, sizeof(fingerprint));
					memset(ck->fp, 0, sizeof(fingerprint));
					ck->id = TEMPORARY_ID;
				}
				TIMER_END(1, jcr.read_chunk_time);
				sync_queue_push(upgrade_chunk_queue, ck);
				TIMER_BEGIN(1);
			}
			if (is_new && i == container_num - 1) {
				if (con_buffer) {
					free_container(con_buffer);
				}
				con_buffer = con;
				break;
			}
			free_container(con);
		}

		ck = new_chunk(0);
		ck->id = c->id;
		SET_CHUNK(ck, CHUNK_CONTAINER_END);
		if (is_new) {
			SET_CHUNK(ck, CHUNK_REPROCESS);
		}
		TIMER_END(1, jcr.read_chunk_time);
		sync_queue_push(upgrade_chunk_queue, ck);
		TIMER_BEGIN(1);

		free(conList);
		// jcr.read_container_num++;
			
		TIMER_END(1, jcr.read_chunk_time);
		sync_queue_push(upgrade_chunk_queue, c);

	}

	FINISH_TIME_RECORD
	if (con_buffer) {
		free_container(con_buffer);
	}
	sync_queue_term(upgrade_chunk_queue);
	return NULL;
}


static void* pre_dedup_thread(void *arg) {
	DECLARE_TIME_RECORDER("pre_dedup_thread");
	while (1) {
		struct chunk* c = sync_queue_pop(upgrade_recipe_queue);

		if (c == NULL) {
			break;
		}

		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END)
			|| destor.upgrade_level == UPGRADE_NAIVE) {
			sync_queue_push(pre_dedup_queue, c);
			continue;
		}

		/* Each duplicate chunk will be marked. */
		pthread_mutex_lock(&upgrade_index_lock.mutex);
		// while (upgrade_index_lookup(c) == 0) { // 目前永远是1, 所以不用管cond
		// 	pthread_cond_wait(&upgrade_index_lock.cond, &upgrade_index_lock.mutex);
		// }
		BEGIN_TIME_RECORD
		if (destor.upgrade_level == UPGRADE_2D_RELATION
			|| destor.upgrade_level == UPGRADE_2D_CONSTRAINED
			|| destor.upgrade_level == UPGRADE_SIMILARITY) {
			if (g_hash_table_lookup(upgrade_processing, &c->id)) {
				// container正在处理中, 标记为duplicate, c->id为TEMPORARY_ID
				// DEBUG("container processing: %ld", c->id);
				SET_CHUNK(c, CHUNK_DUPLICATE);
				SET_CHUNK(c, CHUNK_PROCESSING);
				// c->id = TEMPORARY_ID;
				jcr.sync_buffer_num++;
			} else {
				upgrade_index_lookup(c);
				if(!CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
					// 非重复块需要在下一阶段开始处理, 这个不能放到下面的阶段, 必须在同一锁内
					int64_t *id = malloc(sizeof(int64_t));
					*id = c->id;
					g_hash_table_insert(upgrade_processing, id, "1");
				}
			}
		} else if (destor.upgrade_level == UPGRADE_1D_RELATION) {
			upgrade_index_lookup(c);
		} else {
			// Not Implemented
			assert(0);
		}
		END_TIME_RECORD
		pthread_mutex_unlock(&upgrade_index_lock.mutex);
		sync_queue_push(pre_dedup_queue, c);
	}
	FINISH_TIME_RECORD
	sync_queue_term(pre_dedup_queue);
	return NULL;
}

static void* sha256_thread(void* arg) {
	DECLARE_TIME_RECORDER("sha256_thread");
	// char code[41];
	// 只有计算在container内的chunk的hash, 如果不是2D, 则始终为TRUE
	int in_container = TRUE;
	while (1) {
		struct chunk* c = sync_queue_pop(upgrade_chunk_queue);

		if (c == NULL) {
			sync_queue_term(hash_queue);
			break;
		}

		if (CHECK_CHUNK(c, CHUNK_CONTAINER_START)) {
			in_container = TRUE;
		} else if (CHECK_CHUNK(c, CHUNK_CONTAINER_END)) {
			in_container = FALSE;
		}

		if (!in_container || IS_SIGNAL_CHUNK(c) || CHECK_CHUNK(c, CHUNK_DUPLICATE)) {
			sync_queue_push(hash_queue, c);
			continue;
		}

		BEGIN_TIME_RECORD

		TIMER_DECLARE(1);
		TIMER_BEGIN(1);
		jcr.hash_num++;
		if (CHECK_CHUNK(c, CHUNK_REPROCESS)) {
			// 计算SHA1
			assert(c->id >= 0);
			SHA_CTX ctx;
			SHA1_Init(&ctx);
			SHA1_Update(&ctx, c->data, c->size);
			SHA1_Final(c->old_fp, &ctx);
		} else {
			// 计算SHA256
			assert(c->id == TEMPORARY_ID);
			SHA256_CTX ctx;
			SHA256_Init(&ctx);
			SHA256_Update(&ctx, c->data, c->size);
			SHA256_Final(c->fp, &ctx);
		}
		TIMER_END(1, jcr.hash_time);

		END_TIME_RECORD

		// hash2code(c->fp, code);
		// code[40] = 0;
		// VERBOSE("Update hash phase: %ldth chunk identified by %s", chunk_num++, code);

		sync_queue_push(hash_queue, c);
	}
	FINISH_TIME_RECORD
	return NULL;
}

void do_update(int revision, char *path) {

	init_recipe_store();
	init_container_store();
	init_index();

	init_update_jcr(revision, path);
	pthread_mutex_init(&upgrade_index_lock.mutex, NULL);

	destor_log(DESTOR_NOTICE, "job id: %d", jcr.id);
	destor_log(DESTOR_NOTICE, "new job id: %d", jcr.new_id);
	destor_log(DESTOR_NOTICE, "upgrade_level %d", destor.upgrade_level);
	destor_log(DESTOR_NOTICE, "backup path: %s", jcr.bv->path);
	destor_log(DESTOR_NOTICE, "new backup path: %s", jcr.new_bv->path);
	destor_log(DESTOR_NOTICE, "update to: %s", jcr.path);

	upgrade_recipe_queue = sync_queue_new(100);
	upgrade_chunk_queue = sync_queue_new(100);
	pre_dedup_queue = sync_queue_new(100);
	hash_queue = sync_queue_new(100);

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	puts("==== update begin ====");

    jcr.status = JCR_STATUS_RUNNING;
	pthread_t recipe_t, read_t, pre_dedup_t, hash_t;
	switch (destor.upgrade_level)
	{
	case UPGRADE_NAIVE:
	case UPGRADE_1D_RELATION:
		pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);
		pthread_create(&pre_dedup_t, NULL, pre_dedup_thread, NULL);
		pthread_create(&read_t, NULL, lru_get_chunk_thread, NULL);
		pthread_create(&hash_t, NULL, sha256_thread, NULL);
		break;
	case UPGRADE_2D_RELATION:
	case UPGRADE_2D_CONSTRAINED:
		pthread_create(&recipe_t, NULL, read_recipe_thread, NULL);
		pthread_create(&pre_dedup_t, NULL, pre_dedup_thread, NULL);
		pthread_create(&read_t, NULL, lru_get_chunk_thread_2D, NULL);
		pthread_create(&hash_t, NULL, sha256_thread, NULL);
		break;
	case UPGRADE_SIMILARITY:
		pthread_create(&recipe_t, NULL, read_similarity_recipe_thread, NULL);
		pthread_create(&pre_dedup_t, NULL, pre_dedup_thread, NULL);
		pthread_create(&read_t, NULL, lru_get_chunk_thread_2D, NULL);
		pthread_create(&hash_t, NULL, sha256_thread, NULL);
		break;
	default:
		assert(0);
	}

	if (destor.upgrade_level == UPGRADE_NAIVE || destor.upgrade_level == UPGRADE_1D_RELATION) {
		start_dedup_phase();
		start_rewrite_phase();
	}
	start_filter_phase();

    do{
        sleep(5);
        /*time_t now = time(NULL);*/
        fprintf(stderr, "%" PRId64 " GB, %" PRId32 " chunks, %d files processed, %d files pre_processed\r", 
                jcr.data_size >> 30, jcr.chunk_num, jcr.file_num, jcr.pre_process_file_num);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
    fprintf(stderr, "%" PRId64 " GB, %" PRId32 " chunks, %d files processed\n", 
        jcr.data_size >> 30, jcr.chunk_num, jcr.file_num);

	assert(sync_queue_size(upgrade_recipe_queue) == 0);
	assert(sync_queue_size(upgrade_chunk_queue) == 0);
	assert(sync_queue_size(pre_dedup_queue) == 0);
	assert(sync_queue_size(hash_queue) == 0);

	free_backup_version(jcr.bv);
	update_backup_version(jcr.new_bv);
	free_backup_version(jcr.new_bv);

	pthread_join(recipe_t, NULL);
	pthread_join(pre_dedup_t, NULL);
	pthread_join(read_t, NULL);
	pthread_join(hash_t, NULL);
	if (destor.upgrade_level == UPGRADE_NAIVE || destor.upgrade_level == UPGRADE_1D_RELATION) {
		stop_dedup_phase();
		stop_rewrite_phase();
	}
	stop_filter_phase();

	TIMER_END(1, jcr.total_time);
	puts("==== update end ====");

	close_index();
	close_container_store();
	close_recipe_store();
	pthread_mutex_destroy(&upgrade_index_lock.mutex);

	printf("job id: %" PRId32 "\n", jcr.id);
	printf("update path: %s\n", jcr.path);
	printf("number of files: %" PRId32 "\n", jcr.file_num);
	printf("number of chunks: %" PRId32"\n", jcr.chunk_num);
	printf("total size(B): %" PRId64 "\n", jcr.data_size);
	printf("total time(s): %.3f\n", jcr.total_time / 1000000);
	printf("throughput(MB/s): %.2f\n",
			jcr.data_size * 1000000 / (1024.0 * 1024 * jcr.total_time));
	printf("speed factor: %.2f\n",
			jcr.data_size / (1024.0 * 1024 * jcr.read_container_num));

	printf("read_recipe_time : %.3fs, %.2fMB/s\n",
			jcr.read_recipe_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_recipe_time / 1024 / 1024);
	printf("pre_dedup_time : %.3fs, %.2fMB/s\n", jcr.pre_dedup_time / 1000000,
			jcr.data_size * 1000000 / jcr.pre_dedup_time / 1024 / 1024);
	printf("read_chunk_time : %.3fs, %.2fMB/s\n", jcr.read_chunk_time / 1000000,
			jcr.data_size * 1000000 / jcr.read_chunk_time / 1024 / 1024);
	printf("hash_time : %.3fs, %.2fMB/s\n", jcr.hash_time / 1000000,
			jcr.data_size * 1000000 / jcr.hash_time / 1024 / 1024);
	printf("dedup_time : %.3fs, %.2fMB/s\n", jcr.dedup_time / 1000000,
			jcr.data_size * 1000000 / jcr.dedup_time / 1024 / 1024);
	printf("filter_time : %.3fs, %.2fMB/s\n",
			jcr.filter_time / 1000000,
			jcr.data_size * 1000000 / jcr.filter_time / 1024 / 1024);
	printf("append_thread_time : %.3fs, %.2fMB/s\n",
			jcr.write_time / 1000000,
			jcr.data_size * 1000000 / jcr.write_time / 1024 / 1024);

	char logfile[] = "log/update.log";
	FILE *fp = fopen(logfile, "a");

	/*
	 * job id,
	 * the size of backup
	 * accumulative consumed capacity,
	 * deduplication rate,
	 * rewritten rate,
	 * total container number,
	 * sparse container number,
	 * inherited container number,
	 * 4 * index overhead (4 * int)
	 * throughput,
	 */
	fprintf(fp, "%" PRId32 " %" PRId64 " %" PRId64 " %.4f %.4f %" PRId32 " %" PRId32 " %" PRId32 " %" PRId32" %" PRId32 " %" PRId32" %" PRId32" %.2f\n",
			jcr.id,
			jcr.data_size,
			destor.stored_data_size,
			jcr.data_size != 0 ?
					(jcr.data_size - jcr.rewritten_chunk_size - jcr.unique_data_size)/(double) (jcr.data_size)
					: 0,
			jcr.data_size != 0 ? (double) (jcr.rewritten_chunk_size) / (double) (jcr.data_size) : 0,
			jcr.total_container_num,
			jcr.sparse_container_num,
			jcr.inherited_sparse_num,
			index_overhead.kvstore_lookup_requests,
			index_overhead.lookup_requests_for_unique,
			index_overhead.kvstore_update_requests,
			index_overhead.read_prefetching_units,
			(double) jcr.data_size * 1000000 / (1024 * 1024 * jcr.total_time));

	fclose(fp);

	fp = stdout;
	fprintf(fp, "========== jcr_result ==========\n");
	print_jcr_result(fp);
	fprintf(fp, "========== upgrade_index_overhead ==========\n");
	print_index_overhead(fp, &upgrade_index_overhead);
	fprintf(fp, "========== index_overhead ==========\n");
	print_index_overhead(fp, &index_overhead);
	// fclose(fp);

}
