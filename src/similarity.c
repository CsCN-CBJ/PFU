#include "destor.h"
#include "jcr.h"
#include "update.h"
#include "utils/cache.h"
#include "utils/lru_cache.h"
#include "utils/sync_queue.h"
#include "index/upgrade_cache.h"

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

typedef struct recipeUnit {
	struct fileRecipeMeta *recipe;
	struct chunkPointer *chunks;
	int64_t chunk_off;

	containerid sub_id;
	containerid total_num;
	containerid chunk_num;

	struct recipeUnit *next;
} recipeUnit_t;

void free_featureList(gpointer data) {
	struct featureList *list = data;
	free(list->recipeIDList);
	free(list);
}

int compare_container_id(void *a, void *b) {
	return *(containerid*) a == *(containerid*) b;
}

static void feature_table_insert(GHashTable *featureTable[FEATURE_NUM], feature features[FEATURE_NUM], containerid recipeID) {
	for (int i = 0; i < FEATURE_NUM; i++) {
		struct featureList *list = g_hash_table_lookup(featureTable[i], &features[i]);
		if (!list) {
			list = malloc(sizeof(struct featureList));
			list->count = 0;
			list->max_count = 1;
			list->recipeIDList = malloc(sizeof(containerid) * list->max_count);
			list->feature = features[i];
			g_hash_table_insert(featureTable[i], &list->feature, list);
		} else if (list->count >= list->max_count) {
			list->max_count *= 2;
			list->recipeIDList = realloc(list->recipeIDList, sizeof(containerid) * list->max_count);
		}
		list->recipeIDList[list->count++] = recipeID;
	}
}

static recipeUnit_t *read_one_file(feature features[FEATURE_NUM]) {
	static int file_num = 0;
	if (file_num >= jcr.bv->number_of_files) {
		return NULL;
	}

	int i, j, k;
	for (i = 0; i < FEATURE_NUM; i++) {
		features[i] = LLONG_MAX;
	}

	recipeUnit_t *unit = malloc(sizeof(recipeUnit_t));
	unit->chunk_off = ftell(jcr.bv->recipe_fp);

	int chunknum;
	struct fileRecipeMeta *r = read_next_file_recipe_meta(jcr.bv);
	struct chunkPointer* cp = read_next_n_chunk_pointers(jcr.bv, r->chunknum, &chunknum);
	assert(r->chunknum == chunknum);

	unit->recipe = r;
	unit->chunks = cp;
	unit->next = NULL;
	unit->sub_id = 0;
	unit->total_num = 1;
	unit->chunk_num = r->chunknum;
	
	// calculate features
	for (j = 0; j < r->chunknum; j++) {
		for (k = 0; k < FEATURE_NUM; k++) {
			features[k] = MIN(features[k], CALC_FEATURE(cp[j].id, k));
		}
	}

	file_num++;
	jcr.pre_process_file_num++;
	return unit;
}

static int calculate_unique_container(recipeUnit_t *u, GHashTable *htb) {
	GHashTable *h = htb;
	if (!h) h = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, NULL);
	for (int i = 0; i < u->recipe->chunknum; i++) {
		containerid id = u->chunks[i].id;
		if (g_hash_table_lookup(h, &id)) continue;
		containerid *p = malloc(sizeof(containerid));
		*p = id;
		g_hash_table_insert(h, p, "1");
	}
	int size = g_hash_table_size(h);
	if (!htb) g_hash_table_destroy(h);
	return size;
}

static void CDC_recipe(DynamicArray *array, GHashTable *featureTable[FEATURE_NUM], recipeUnit_t *u) {
	assert(destor.CDC_exp_size - destor.CDC_min_size > 0);
	int64_t currentSubID = 0, startArrayIndex = array->size, startChunkIndex = 0;
	GHashTable *cdcTable = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, NULL);
	for (int i = 0; i < u->recipe->chunknum;) {
		g_hash_table_remove_all(cdcTable);
		// 创建新的recipeUnit
		feature subFeatures[FEATURE_NUM];
		for (int k = 0; k < FEATURE_NUM; k++) {
			subFeatures[k] = LLONG_MAX;
		}
		recipeUnit_t *sub = malloc(sizeof(recipeUnit_t));
		sub->recipe = copy_file_recipe_meta(u->recipe);
		sub->next = NULL;
		sub->sub_id = currentSubID++;
		startChunkIndex = i;

		// 开始填充当前子块
		for (; i < u->recipe->chunknum; i++) {
			if (g_hash_table_size(cdcTable) >= destor.CDC_max_size) break;

			// 插入当前chunk
			containerid id = u->chunks[i].id;
			if (g_hash_table_lookup(cdcTable, &id)) continue;
			containerid *p = malloc(sizeof(containerid));
			*p = id;
			g_hash_table_insert(cdcTable, p, "1");
			// 计算feature
			for (int k = 0; k < FEATURE_NUM; k++) {
				subFeatures[k] = MIN(subFeatures[k], CALC_FEATURE(id, k));
			}
			
			// 跳过min个container
			if (g_hash_table_size(cdcTable) < destor.CDC_min_size) continue;

			// 进行CDC
			if (id % (destor.CDC_exp_size - destor.CDC_min_size) == 0) break;
		}

		// 继续插入重复chunk
		for (; i < u->recipe->chunknum; i++) {
			if (!g_hash_table_lookup(cdcTable, &u->chunks[i].id)) break;
		}

		// 插入到recipeList
		sub->chunk_num = i - startChunkIndex;
		// sub->chunks = malloc(sizeof(struct chunkPointer) * sub->chunk_num);
		// memcpy(sub->chunks, u->chunks + startChunkIndex, sizeof(struct chunkPointer) * sub->chunk_num);
		sub->chunks = NULL;
		assert(u->chunk_off % (sizeof(fingerprint) + sizeof(containerid) + sizeof(int32_t)) == 0);
		sub->chunk_off = u->chunk_off + startChunkIndex * (sizeof(fingerprint) + sizeof(containerid) + sizeof(int32_t));

		feature_table_insert(featureTable, subFeatures, array->size);
		dynamic_array_add(array, sub);
		WARNING("Sub recipe %d, chunk num %d unique %d %s", sub->sub_id, sub->chunk_num, g_hash_table_size(cdcTable), u->recipe->filename);
		jcr.logic_recipe_unique_container += g_hash_table_size(cdcTable);
	}
	int total = 0;
	for (int i = startArrayIndex; i < array->size; i++) {
		((recipeUnit_t *)array->data[i])->total_num = currentSubID;
		total += ((recipeUnit_t *)array->data[i])->chunk_num;
	}
	assert(total == u->recipe->chunknum);
}

static int process_recipe(recipeUnit_t ***recipeList, GHashTable *featureTable[FEATURE_NUM]) {
	assert(destor.CDC_max_size >= 2 * destor.CDC_min_size); // 确保两个小文件合并后不会超过最大容量
	DynamicArray *array = dynamic_array_new();
	GHashTable *cacheTable = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, NULL);
	recipeUnit_t *cacheListHead = NULL;
	recipeUnit_t *cacheListTail = NULL;
	feature cacheFeatures[FEATURE_NUM];
	int merge_unique_num;
	
	while (1) {
		feature features[FEATURE_NUM];
		recipeUnit_t *u = read_one_file(features);
		if (!u) break;

		int unique_num = calculate_unique_container(u, NULL);
		jcr.physical_recipe_unique_container += unique_num;
		// 基础版不进行切分合并
		if (!destor.upgrade_do_split_merge) {
			feature_table_insert(featureTable, features, array->size);
			dynamic_array_add(array, u);
			free(u->chunks);
			u->chunks = NULL;
			continue;
		}

		if (unique_num < destor.CDC_min_size) {
			// merge files
			if (!cacheListHead) {
				cacheListHead = u;
				cacheListTail = u;
			} else {
				cacheListTail->next = u;
				cacheListTail = u;
			}
			for (int i = 0; i < FEATURE_NUM; i++) {
				cacheFeatures[i] = MIN(cacheFeatures[i], features[i]);
			}
			
			merge_unique_num = calculate_unique_container(u, cacheTable);
			assert(merge_unique_num <= destor.CDC_max_size);
			// 如果合并后的文件仍然小于最小容量, 则继续等待合并
			if (merge_unique_num < destor.CDC_min_size) {
				WARNING("Merge file %s unique_num: %d, merge_unique_num: %d", cacheListHead->recipe->filename, unique_num, merge_unique_num);
				continue;
			}
			WARNING("Merge end %s unique_num: %d, merge_unique_num: %d", cacheListHead->recipe->filename, unique_num, merge_unique_num);

			// add to recipeList
			feature_table_insert(featureTable, cacheFeatures, array->size);
			dynamic_array_add(array, cacheListHead);
			
			// reset cache
			g_hash_table_remove_all(cacheTable);
			cacheListHead = NULL;
			cacheListTail = NULL;
			for (int i = 0; i < FEATURE_NUM; i++) {
				cacheFeatures[i] = LLONG_MAX;
			}
			jcr.logic_recipe_unique_container += merge_unique_num;
			free(u->chunks);
			u->chunks = NULL;
		} else if (unique_num > destor.CDC_max_size) {
			WARNING("file %s Unique num %d exceed max size %d", u->recipe->filename, unique_num, destor.CDC_max_size);
			CDC_recipe(array, featureTable, u);
			free_file_recipe_meta(u->recipe);
			free(u->chunks);
			free(u);
		} else {
			WARNING("file %s Unique num %d", u->recipe->filename, unique_num);
			feature_table_insert(featureTable, features, array->size);
			dynamic_array_add(array, u);
			jcr.logic_recipe_unique_container += unique_num;
			free(u->chunks);
			u->chunks = NULL;
		}
	}

	// add the last cache
	if (cacheListHead) {
		feature_table_insert(featureTable, cacheFeatures, array->size);
		dynamic_array_add(array, cacheListHead);
		jcr.logic_recipe_unique_container += merge_unique_num;
	}

	*recipeList = array->data;
	int size = array->size;
	free(array);
	return size;
}

static void send_one_recipe(SyncQueue *queue, recipeUnit_t *unit, feature featuresInLRU[FEATURE_NUM], struct lruCache *lru) {
	assert(destor.upgrade_level == UPGRADE_SIMILARITY);
	
	struct fileRecipeMeta *r = unit->recipe;
	assert(unit->chunks == NULL);
	struct chunkPointer *cp = read_n_chunk_pointers(jcr.bv, unit->chunk_off, unit->chunk_num);

	// 发送recipe
	struct chunk *c = new_chunk(2 * sizeof(containerid) + sdslen(r->filename) + 1);
	((containerid *)c->data)[0] = unit->sub_id;
	((containerid *)c->data)[1] = unit->total_num;
	strcpy(c->data + 2 * sizeof(containerid), r->filename);
	SET_CHUNK(c, CHUNK_FILE_START);
	sync_queue_push(upgrade_recipe_queue, c);

	count_cache_hit(cp, unit->chunk_num);

	for (int k = 0; k < FEATURE_NUM; k++) {
		featuresInLRU[k] = LLONG_MAX;
	}

	for (int i = 0; i < unit->chunk_num; i++) {
		// 遍历recipe中所有chunk, 对其containerid进行特征计算
		struct chunk* c = new_chunk(0);
		memcpy(&c->old_fp, &cp[i].fp, sizeof(fingerprint));
		c->size = cp[i].size;
		c->id = cp[i].id;
		sync_queue_push(upgrade_recipe_queue, c);

		for (int k = 0; k < FEATURE_NUM; k++) {
			featuresInLRU[k] = MIN(featuresInLRU[k], CALC_FEATURE(cp[i].id, k));
		}

		// simulate LRU
		/*
		// 如果找到, 会自动将其移到头部
		containerid *id_p = lru_cache_lookup(lru, &c->id);
		if (id_p) continue;
		// 插入新的containerid
		// 如果在container hashtable中找到, 实际上对于LRU来说和一个新的container是一样的, 不需要额外处理
		id_p = malloc(sizeof(containerid));
		*id_p = c->id;
		for (int k = 0; k < FEATURE_NUM; k++) {
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
		*/
	}

	c = new_chunk(0);
	SET_CHUNK(c, CHUNK_FILE_END);
	sync_queue_push(upgrade_recipe_queue, c);

	free_file_recipe_meta(r);
	free(cp);
}

void send_recipe_unit(SyncQueue *queue, recipeUnit_t *unit, feature featuresInLRU[FEATURE_NUM], struct lruCache *lru) {
	if (unit->next) {
		WARNING("Send merge recipe start");
		while (unit) {
			WARNING("Send %s %ld/%ld", unit->recipe->filename, unit->sub_id + 1, unit->total_num);
			send_one_recipe(queue, unit, featuresInLRU, lru);
			recipeUnit_t *temp = unit;
			unit = unit->next;
			free(temp);
		}
		WARNING("Send merge recipe end");
	} else {
		WARNING("Send %s %ld/%ld", unit->recipe->filename, unit->sub_id + 1, unit->total_num);
		send_one_recipe(queue, unit, featuresInLRU, lru);
		free(unit);
	}
}

void* read_similarity_recipe_thread(void *arg) {

	int i, j, k;
	fingerprint zero_fp;
	memset(zero_fp, 0, sizeof(fingerprint));
	recipeUnit_t **recipeList;
	// list [ hashtable [ feature -> featureList[ recipe id ] ] ]
	GHashTable *featureTable[FEATURE_NUM];
	for (i = 0; i < FEATURE_NUM; i++) {
		featureTable[i] = g_hash_table_new_full(g_int64_hash, g_int64_equal, free_featureList, NULL);
	}

	// read all recipes and calculate features
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	int recipe_num = process_recipe(&recipeList, featureTable);
	TIMER_END(1, jcr.read_recipe_time);

	// send recipes
	struct lruCache *lru = new_lru_cache(destor.index_cache_size, free, compare_container_id);
	feature featuresInLRU[FEATURE_NUM] = { LLONG_MAX, LLONG_MAX, LLONG_MAX, LLONG_MAX };
	GHashTable *sendedRecipe = g_hash_table_new_full(g_int64_hash, g_int64_equal, free, NULL);
	for (i = 0; i < recipe_num; i++) {
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
		WARNING("Best recipe %ld %ld", bestRecipeID, bestRecipeRef);
		// 如果没有找到任何相似的recipe, 选择第一个未发送的recipe
		if (bestRecipeID == -1) {
			for (containerid id = 0; id < recipe_num; id++) {
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
		g_hash_table_insert(sendedRecipe, recipeID_p, "1");

		// 发送recipe
		recipeUnit_t *unit = recipeList[bestRecipeID];
		send_recipe_unit(upgrade_recipe_queue, unit, featuresInLRU, lru);
	}

	sync_queue_term(upgrade_recipe_queue);
	free_lru_cache(lru);
	g_hash_table_destroy(sendedRecipe);
	for (i = 0; i < FEATURE_NUM; i++) {
		g_hash_table_destroy(featureTable[i]);
	}
	free(recipeList);
	return NULL;
}
