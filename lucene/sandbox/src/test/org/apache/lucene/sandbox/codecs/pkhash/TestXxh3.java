/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.sandbox.codecs.pkhash;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestXxh3 extends LuceneTestCase {

  /** Lengths beyond {@code [0, 260]} that the golden values cover. */
  private static final int[] LONG_LENGTHS = {
    300, 500, 1023, 1024, 1025, 1088, 1089, 2048, 4096, 32766
  };

  /**
   * The inputs of the golden values: the first {@code length} bytes of this, where byte {@code i}
   * is {@code (byte) (i * 31 + 7)}.
   */
  private static byte[] goldenInput(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (i * 31 + 7);
    }
    return bytes;
  }

  /**
   * Every length of {@code [0, 260]}, then {@link #LONG_LENGTHS}: the order of the golden values.
   */
  private static int[] goldenLengths() {
    int[] lengths = new int[261 + LONG_LENGTHS.length];
    for (int i = 0; i <= 260; i++) {
      lengths[i] = i;
    }
    System.arraycopy(LONG_LENGTHS, 0, lengths, 261, LONG_LENGTHS.length);
    return lengths;
  }

  /*
   * Golden values: XXH3_64bits_withSeed of the golden inputs, for every length of goldenLengths(),
   * with seed 0 and with TermHash.SEED, generated with python's xxhash 3.6.0 (libxxhash 0.8.2):
   *
   *   import xxhash
   *   lengths = list(range(261)) + [300, 500, 1023, 1024, 1025, 1088, 1089, 2048, 4096, 32766]
   *   data = bytes((i * 31 + 7) & 0xFF for i in range(32766))
   *   [xxhash.xxh3_64_intdigest(data[:n], seed=seed) for n in lengths]
   *
   * They cover every code path: the empty input, 1-3, 4-8, 9-16, 17-128 (each of its four
   * branches), 129-240 bytes, and longer inputs with zero to 31 full blocks of 1024 bytes and 1, 2,
   * 4, 5, 8 or 16 stripes in the last block. But no byte of the inputs of 1 to 3 bytes is 0x80 or
   * more, and every 256 bytes of the input are the same, so the full blocks of an input are all the
   * same too, whatever their order: the mixed golden values below cover that.
   */

  private static final long[] SEED_0 = {
    0x2d06800538d394c2L, 0x4c5cca45d0f4811fL, 0xa7e250c97710ff27L, 0x15f7093b173d005cL,
    0xdca012f95811b6b9L, 0xb290cafc7b254345L, 0x99b2e675fba1e0b5L, 0x7561869c23da3c1bL,
    0xdec6a9a43575982eL, 0xcbe393399f17ffbdL, 0x02ab330b44074ad1L, 0x32f214df8eed29c2L,
    0x46aaf92c7550afa4L, 0x9e314510b0dbd95cL, 0x11c33b5547089febL, 0x545e19990471dc37L,
    0x7e484c18d74895d0L, 0x208bde5ee2bed407L, 0x0b2fc9efca72f11fL, 0x4a8e9636611ef195L,
    0xb752b00444280fd5L, 0xc0c251a7aaa88000L, 0x00bec0f929fb1424L, 0xf1ed4292dd803ac6L,
    0x48953b9e46ea32c5L, 0x2cb743cc698a6b5bL, 0xbe451d7ce35fa496L, 0x482ddc5d1c67b259L,
    0x711a5d15799c5096L, 0x77faf0f24f702d64L, 0x96a2eca234677819L, 0xa937652b0119ca11L,
    0x03df0ac5255d1446L, 0x199a362122d71f46L, 0x502358051cdae375L, 0x3979f3a31317573bL,
    0x362c6fed990fb45fL, 0xe07bdcb266833293L, 0x3ceb2f4c443330ceL, 0x35590c4b1b716079L,
    0x80ef652ab3f1b4b5L, 0xf88aad891ae21edcL, 0x491e0741086bc8b1L, 0x7faba0b013e5699bL,
    0x63cc939553a0f673L, 0xfef1383ea53f9386L, 0xffaa39765685d4d8L, 0x0b9d89b90ef6f825L,
    0x37fc8c692bc274e1L, 0x600574020639d802L, 0x98eac754de954c04L, 0x4ef18bcbbbe5c7b4L,
    0xb618d180fb23c0e7L, 0xa88e3182010cacd3L, 0xe670fa5605d0bbcfL, 0x1fc37b65eeb8ce92L,
    0x3c837cc3c15a01bdL, 0x5298171a4b2c5766L, 0xa4f0435a673b9650L, 0xa12450aededa97c1L,
    0x5b53af902b44d30eL, 0x104cc689e1c6359fL, 0xc7415ebecf37be00L, 0x76d4eec1f092847fL,
    0xdd30702ab46b3745L, 0xfab36b851b94ce20L, 0x7ed877eaeb7ab09eL, 0x5679e93ebb6b4667L,
    0x48a7f2c0bb6660d6L, 0x5b605745067f8f78L, 0xd254549641ca7cdaL, 0x6b020d44d649cd18L,
    0x8a3f6fb70e7f3c5aL, 0xd63ad17c03358856L, 0xadb5d6b84bb0c7e6L, 0xf38e445be5bb7774L,
    0x924ae4523fda2623L, 0x80af1a82d31f26edL, 0x5bae338f3410811fL, 0x2db5e7edf9911248L,
    0xbab4673ab248e1e2L, 0xadbb20e609fbccf3L, 0x914a22ae0735c26cL, 0xd310fea8290adf1cL,
    0xef3d3650aeba996dL, 0xa6b8f2b76480c80fL, 0x422f63ca8db1dc58L, 0x578e0991378b0edbL,
    0x56b28c120f398e4eL, 0xb1b9cd0d8836c6e5L, 0x8bf0778b4af4060aL, 0x6c1f0cdee38bbe57L,
    0xc8c7b7a3f485984cL, 0xbb1e53010e1fe285L, 0xa381f91d6391149bL, 0x9857e7a5a42c6fe9L,
    0xd245cd2541582982L, 0x60e3e1d0d43785b3L, 0x5d08d66481f2ca1dL, 0x5e2543d8ebdd1946L,
    0x8c97158042fbf926L, 0xd2fdb1aa73b78a0aL, 0xd9ef45a695356955L, 0x22e5af597a44ac05L,
    0xd3dce276c955e780L, 0x77b5b20585cb1848L, 0xfa0d562d176e7144L, 0x763f0966982e0c4eL,
    0xbaa3a573cadfa833L, 0x56f9c083f935b104L, 0x60bca2d0a693e0ffL, 0x22d06ad6c0216ec3L,
    0xdbae773438781980L, 0x18f12e161c06865bL, 0xc48fc7c4e67f0a55L, 0x5394b3c6f33d6c02L,
    0xf9dd9e1211e25d2dL, 0x7044bacb399d52caL, 0xa97b6c41a31ca72dL, 0xd1fa6a6c33ed184cL,
    0xdbb9fc4818080ebaL, 0x5884cd0cf2cb9749L, 0x866a55eee34e87b1L, 0x08649efe402a8b29L,
    0x71ec3fce38b2ea36L, 0x65e3707c889a12d0L, 0x71e3999204f56048L, 0xa915ed6396db8cc0L,
    0xf92b70eaa21a6288L, 0xf8f76713f2bb60faL, 0x303a2c3823c3c34aL, 0x9ddf75554d4ef43fL,
    0xec1ecd2020110609L, 0x8374b8d42931f8f1L, 0x94dfabf82db326cfL, 0x54659005622c9884L,
    0xa62bec58f8de6b28L, 0xa1eccf34d87679abL, 0xe1a6abf613d0c713L, 0xd4d888ea09ad8be2L,
    0x73f9bf9367a34a5aL, 0x265cb82209b57673L, 0xc56b3ae0803aa7f2L, 0xc372f52c27156971L,
    0xd87f00765ee3d89eL, 0x65d480417b4734f8L, 0x86c2036ce5e060f8L, 0xd07b5c2f0fefd8a8L,
    0x91dbbdae16d82b72L, 0x7749c551163f6b04L, 0xb87aa95cb5f54aa2L, 0x01246a53c1e24e38L,
    0x665c91b034d599beL, 0xee6ddbe8d073c666L, 0x2f8d3364d5dfccc8L, 0x11f85d6cd9966443L,
    0x15f260ba4b1e0968L, 0x063b1ecd55a72930L, 0x25bb4a9fb16a2d48L, 0xa327b7cae27c8e10L,
    0xc90911ffcef461e2L, 0xf11ed84f70bade2eL, 0xde69b26de1a9b3f2L, 0x0ae8762e72422656L,
    0x4b00656e32461c31L, 0x7ba0c08511bbe8b3L, 0x27cda8258d9b1179L, 0x87b3e083141f3409L,
    0x3a62b95662105e41L, 0x01b490d2d915abadL, 0x643721c9bf32781cL, 0x8287c649ce43e397L,
    0xa3dae8f46d8fd330L, 0xc1f47864f99336fcL, 0x5c0072559c0dd67bL, 0xbfdab869a2187b30L,
    0x67f10aaf46d79a44L, 0xa8ece1c90e74efbfL, 0x50887a99d15a2cfaL, 0x32da48475d1b8ad5L,
    0xbb150b04dbb71027L, 0xce006955e8ea3d6aL, 0x3d8cc59b34a1d734L, 0x8807c2bf48aadebcL,
    0x530061af42546bfaL, 0x955b87dacb465bb7L, 0x82f6fee48cc701c4L, 0x338ed6d7173ffe5dL,
    0xe29ffc4b065c0c89L, 0x0fe5c2e35a759ae3L, 0x2f77a9373bb6e149L, 0xf0025e0992b308fbL,
    0x084028fa06f01d30L, 0xf0eb08bf31ca42b6L, 0x88510eba2c60efe4L, 0x5ea76d72c792b501L,
    0x69a29b581ae42494L, 0x7b7dcee881e9ea8cL, 0x3622e29db96318fcL, 0x8124966e9386ec04L,
    0x12fdb864685f344dL, 0x16101b47f8644472L, 0x4a2dec05a37c91a0L, 0x0fac1fabb0a5293aL,
    0x0e1f9008494d4fd7L, 0xdc66165d9b20e180L, 0x1718ea095c340389L, 0x5f43962e18eca50eL,
    0x80826d7a65be5e69L, 0x6a4f9f953de8f7e4L, 0xafcc79d2b952690bL, 0xb95f2c918d8fa761L,
    0xde10988270de16b4L, 0x7f939839dd0460ddL, 0x71c112f5c1a6c2ecL, 0x2b3d2c4e3dca7a16L,
    0x7de819be3e17b031L, 0x24667ff21981291cL, 0x403cec6b6dde22fdL, 0xa21db8d0bfda5b12L,
    0x72dc002165c95f3cL, 0x5efc45722724a76fL, 0xbeff6aef7d2c8900L, 0x6e8f4c03accab5a4L,
    0xe69e5c66a23443bcL, 0xd01a4f7215895c93L, 0x33cd79ebc1406b8eL, 0xd16389c3956e82dbL,
    0x8ba30402067714e9L, 0xefb75e81f42711c2L, 0xb73ba8942ee8ec15L, 0x90b17ddb0b598b06L,
    0x1eb54c570c6806ffL, 0x409daebb5cf1cbe0L, 0x7f230cb54edc2575L, 0x89aa4d3171c051baL,
    0xa2c90d7776c670ebL, 0xc80c97d6950b23f5L, 0x7f477f909665b010L, 0xcaa9b7a588464745L,
    0xccc7375172c41f03L, 0x0b3b630948ce4a00L, 0x64ac2c6712669e05L, 0xcffaa40f5965922dL,
    0x79217054e33ac2a4L, 0xbbedbcb1301bb022L, 0x39b4015e13725a91L, 0x7241d3e647366786L,
    0x81d90928ac0eeb66L, 0x2f4f47dd7c3957d9L, 0x359fe5d526851d06L, 0x0ea0f7ed02748029L,
    0xa9ccd8780a626691L, 0xaea766e710bb3aebL, 0x51b4770c44653ef6L, 0x89932170686cdd9aL,
    0xec85b75bafe6ca74L, 0x12ef0ff633841459L, 0xa4e6408d09e952e3L, 0x954bb7c819d7c692L,
    0x4150449c54ae2818L, 0xbaad8e6a5f186ce6L, 0xe1e3ce93f69c5043L, 0xf0d330ce2b3300fbL,
    0x23bc880ebf0d29c6L, 0xc09fdfbc398c7d82L, 0xa16bad67843c5a45L, 0x822327a86aad957cL,
    0x19f6f9c987331373L, 0xa3c19f8174cde0bbL, 0x3fdfc1fa35429999L,
  };
  private static final long[] SEED_TERMHASH = {
    0x64ce21f217f362b1L, 0xcca5a994fe7ce9abL, 0x0d37b242c77ac0b7L, 0xe4eaddaf71d0efd1L,
    0x0fe8459310aedf82L, 0x25151f6058f7a4cfL, 0x470b14b073048de8L, 0xb79c523222e7f5c8L,
    0x0b916bafe01d532fL, 0x4f2b204d47aa28d8L, 0x92589139aa86362fL, 0x92226d0d45ccd8a6L,
    0xa5b4a09f836dea3fL, 0x67e127659404864aL, 0x63fe08046ec2b175L, 0xb01239e8e867dcbcL,
    0xb3eefadcc2895ff6L, 0xd712b4abe5bae99cL, 0x1592a0289ca0290cL, 0x20816471cc3bea20L,
    0x9be938946f70d7a9L, 0x685d6d25f9f59636L, 0x253238ede8239f21L, 0xc4e35ba68492fca0L,
    0x1fdad9612857f3c1L, 0x24c58210429cf8e4L, 0x2fe48174f43f78cbL, 0x4acfbd6783b56a59L,
    0x34b8d30b8fb624b5L, 0x88a79549627723feL, 0x1b383f9f2f7963e5L, 0x45ca4cbdbcc7e180L,
    0x1dd0af3df3ef3cd7L, 0xa85ccb801141dcbdL, 0x7dcc7cdedbbced6cL, 0xb4fd07fb2df13f23L,
    0x3dac7305dfe9a5e6L, 0xdd0df583e2403226L, 0x000007a15adfd2f2L, 0x2eba726a409d7f12L,
    0x6b4c3a2b47119c82L, 0x2215ca5c79ac10d2L, 0xfd569c09bade0714L, 0xf714802dd18bb201L,
    0xd082f631e1d11bf8L, 0x50653619c8921562L, 0x135e0670f54daa3eL, 0x56f2172f97030fbaL,
    0x2cd8874672295ed6L, 0x117123d87cb1603cL, 0x8b92611b0be2c325L, 0x0dbd6790aa767c1eL,
    0xb7b4c3169ce19bffL, 0x4cfff9c51ef8643bL, 0x619718f9e964c903L, 0x806564b74e6b841dL,
    0x0d12f1f4a5d055d8L, 0x1b40f0749454adcbL, 0x0b4d5c40365aae87L, 0x9e5376b957633933L,
    0x2d58e3db0e1bb72fL, 0x19dc6d427a09399fL, 0x53e3495d6e56242cL, 0xb8cc3c1ecde8fa89L,
    0x9c1913464f0610f3L, 0x51c706df515a1267L, 0x7eb3801ab8c8c571L, 0x006e7a71aafefc75L,
    0x8794eb4b68327840L, 0x6eccd9242458fa03L, 0xca51d1daa29f561dL, 0xad7082918ec67ea2L,
    0xff314fa653079debL, 0x64bed090eca31c17L, 0xa6b193e9b7157a69L, 0x7bdc607b45ec742fL,
    0x8b6db7e95da2e342L, 0x8ff8193aeec85131L, 0x001c9710104dd3bfL, 0x1798f6a154b3f92aL,
    0x9c9a6fcfb2259004L, 0x55c1363a2346958bL, 0xeb10052763d52dd0L, 0x8315b2287d731569L,
    0x1e3a1f1ebb1415f3L, 0xfd5b56655f2154edL, 0x7f87cb4ae358dbd0L, 0x7e750fdfed3920f5L,
    0x05d58b5349cd79baL, 0x76c6459831ea149bL, 0x5d91c08666d1e35fL, 0xc040f13a3ff3fc8aL,
    0xb9a0db47719c62f3L, 0xcc989cd9ab839307L, 0x0ca2fcebb3223183L, 0x11af0caba23d1202L,
    0xcd67fe056e26a2efL, 0x35673f71e8b47f77L, 0x7dce500ef277ef09L, 0x3f861892bfc390a2L,
    0xeb4f1f687f6f79a3L, 0x4f663dbc196fc07dL, 0x3d346d7fdf2827b9L, 0xe36e0c946279a0bcL,
    0x2066e3878848d2e6L, 0x41a851615ce4f397L, 0xa9a793e378361984L, 0xd5bc0ce5bd95c135L,
    0x656f3b2e1312a72fL, 0x62913343cf5b683cL, 0x4e54464031aabeb5L, 0x6b0f200e43a834b2L,
    0x8284da3187be154bL, 0x213fed34ebf978a7L, 0x013132d59c8d2937L, 0x5e89e8e830d5d898L,
    0x621ada2cfcefaaf6L, 0xa9602da96624f48dL, 0x23ea8099a5c51b46L, 0x13b28e99b00f0cd2L,
    0xe3350f628291a7c8L, 0xbfd457e34bc5ea1aL, 0x2bbb1002d50d3473L, 0x5b1406e5c65201c7L,
    0x0383f057f584056fL, 0x33dcf8d8a418ccdbL, 0x4032673954fb97b3L, 0xf70ecd91bc324052L,
    0x14712a996032fd34L, 0x1c21869011d8d9f5L, 0xbdc3cf619f31ea3eL, 0x12afd2c896c06fe4L,
    0xde2ef937a673528cL, 0xc74d98421f2aa79bL, 0x91e41012b4ccc1bdL, 0x4d7727dcedb483faL,
    0x19efd55a66a03462L, 0x733090dc038165deL, 0x23d9d2f1f9f46398L, 0x264f07c083dafcbaL,
    0x8518fceb99f6a2a6L, 0xb863add62c20e35dL, 0x1e73f91cb624c685L, 0x904347dc19f9c694L,
    0x33efab7c9cf6e8abL, 0x786c3c804c3497cdL, 0x825d622a2623511bL, 0xf2ee2329a3062016L,
    0xa9ea826c32f8aab4L, 0xd3dc69705e6f231bL, 0xf2de7733dafb26d6L, 0x0185021d8aa11bd4L,
    0xb2e24bf515a77a9dL, 0x7e9aa5334f9f0e59L, 0xd16efbd1073f56d1L, 0x18a4e5c35d420c5bL,
    0x4b7ae886be2f3991L, 0xa694c2ad2991f81fL, 0xcd10cf936f382845L, 0xcf6e9acf3ce6ede3L,
    0x9076de96e871c63fL, 0xe49a7406d949587eL, 0xcbac866adbd5daa8L, 0x51ab3b4002b27c5cL,
    0xd3965346b4b424c5L, 0x81081a8613a7cef1L, 0xfec2c2dd8f34c483L, 0xd275f152e0fd49cbL,
    0x5cdcebbdeb56e1edL, 0x2edda603cefd925eL, 0x18a98b18f5ffd723L, 0x578bbc2175881ee4L,
    0xe9d5b27e9f0f1110L, 0xf2f5c2c58f9acbd5L, 0x6583e58c0aa99ec6L, 0xfe958c34690e8a67L,
    0x8d69600c889bc4e8L, 0xf20522a7b9a9b692L, 0xd75cc28ffef26287L, 0x68c1a65e23b9ee09L,
    0xb90907244ad5a207L, 0xcd1623140a533039L, 0x9f5ea6dc488e4424L, 0x52220d3d2eadbd3aL,
    0x709da66009fd509eL, 0xd5c339c202c0b2c1L, 0x90d35c6a8f9d756aL, 0x345e83256488ec58L,
    0x0eb13ae6cf286fd8L, 0xdea8cbaa006610b8L, 0x4ea6a171f7f27c28L, 0xf01cb6a570226849L,
    0x00bf19baaa44dbedL, 0x6671785ef1a2ea95L, 0xe0448cbbfa4f8eeaL, 0x1322cfcadea1419fL,
    0xf560304fbacd7f49L, 0x358a1b23cd092205L, 0x47985a874ebd531cL, 0x40901a1a5d5a823dL,
    0x6f1dc4dea4cc4d0bL, 0xe759a5fddb3f036aL, 0xd2cca61fbdfbdd60L, 0x298360f6baa5dc8dL,
    0x23817f9aeefba252L, 0x23e76d0f3dfde397L, 0x98981e2749f7448eL, 0x60c2e3c6ca46d1ebL,
    0x8942905a51ccc8fdL, 0xb5b86d5fa0d0b904L, 0x6ff8ad89835fbde2L, 0x7cae90799e8d1472L,
    0x8b10fcb7e9ca0c8aL, 0x8e74afdf328f879fL, 0x15749aba19108081L, 0x6484547c5de8bae9L,
    0x1e370fd7e12a9714L, 0x61c641742fbf7890L, 0x3eb579ec21394bfdL, 0x6830263ecbe76a7bL,
    0xd0306d3406089c65L, 0x7223d5d081247bb6L, 0x8c10dc5da9efc852L, 0x145328fd8782f43dL,
    0xb15de4505db98f98L, 0xa40691438d9b424eL, 0x5b6d01737fb71c9cL, 0x22ea111b50913f82L,
    0xa70be9bc71b90ea9L, 0x5dc1ce1f1fe8afe3L, 0x481e8f8771a98ef1L, 0xa2f4f6439a1860c9L,
    0xd11efd9e114f8c3eL, 0x60886befe2277beeL, 0xba184a4496d05727L, 0x632ab736cdc762ddL,
    0xb4b06085d0bf72d4L, 0xb21f277a8396cf97L, 0xeb7dff22e65ccf97L, 0x1168e537bd1e1432L,
    0x7548f42eb67e59f2L, 0xc2834d52b6e32ec1L, 0xea862067b2a26169L, 0x5c79e79d06b85925L,
    0x0c093ea5f8af537eL, 0x5814c6bf577a053eL, 0x5ce1dbf7c150d8faL, 0xa94aee386449484bL,
    0x2198eec273bf395dL, 0x8b91a3618d539554L, 0x24eb42f11db69b43L, 0x556eda469df7c4a6L,
    0xbeb34e2c69061813L, 0x59a43cf82eb03123L, 0x8fa126b1c521ee39L, 0x5a19b379f01b7350L,
    0x227391ce6d5cced6L, 0x0d4b650db3b5cf5fL, 0x2baf50cd7e9e4f96L, 0xccf3121928bf85b9L,
    0x68fa61a0cd0fff42L, 0xb5a3892c82327ff2L, 0x88197069e6bcc137L, 0x621bc9e7e5cc6219L,
    0x9248894fbdddcb38L, 0x4a202108e658f7f8L, 0xf1ecf146447ab49dL, 0x32ea1683539fae8bL,
    0x4e02dd71bd35fe9aL, 0x997ac9e0a58f36c8L, 0x7d41afbad1dbcf9aL,
  };

  /**
   * The lengths of the mixed golden values: every length of {@code [0, 16]}, a few up to 1024 (each
   * branch of 17-128 and both ends of 129-240), lengths with one full block and each number of
   * stripes from 1 to 16 in the last block, then several full blocks.
   */
  private static final int[] MIXED_LENGTHS = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 32, 33, 64, 65, 96, 97, 128, 129,
    200, 240, 241, 500, 1024, 1038, 1115, 1192, 1269, 1282, 1359, 1436, 1513, 1590, 1603, 1680,
    1757, 1834, 1911, 1924, 2001, 2048, 3000, 4096, 32766
  };

  /**
   * The inputs of the mixed golden values: the first {@code length} bytes of the little-endian
   * outputs of splitmix64 from a state of 0. They do not repeat, and start with bytes of 0x80 or
   * more (0xAF, 0xCD), which the arithmetic of inputs of 1 to 3 bytes must not sign-extend.
   */
  private static byte[] mixedInput(int length) {
    final byte[] bytes = new byte[length];
    long state = 0;
    for (int i = 0; i < length; i += Long.BYTES) {
      state += 0x9E3779B97F4A7C15L;
      long z = state;
      z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
      z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
      z ^= z >>> 31;
      for (int j = 0; j < Long.BYTES && i + j < length; j++) {
        bytes[i + j] = (byte) (z >>> (j * Byte.SIZE));
      }
    }
    return bytes;
  }

  /*
   * Mixed golden values: XXH3_64bits_withSeed of the mixed inputs, for every length of
   * MIXED_LENGTHS, with seed 0 and with TermHash.SEED, generated with python's xxhash 3.6.0
   * (libxxhash 0.8.2):
   *
   *   import xxhash
   *   M = (1 << 64) - 1
   *   def splitmix64(n):
   *       out, state = bytearray(), 0
   *       while len(out) < n:
   *           state = (state + 0x9E3779B97F4A7C15) & M
   *           z = state
   *           z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & M
   *           z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & M
   *           out += (z ^ (z >> 31)).to_bytes(8, 'little')
   *       return bytes(out[:n])
   *   data = splitmix64(32766)
   *   [xxhash.xxh3_64_intdigest(data[:n], seed=seed) for n in MIXED_LENGTHS]
   */

  private static final long[] MIXED_SEED_0 = {
    0x2d06800538d394c2L, 0x56c67cd7bde2aa02L, 0x8fd781ebf2f4e55aL, 0xa01aad385609e8c7L,
    0xe459f345944e09e5L, 0x766659d19847c180L, 0x4ca57f6dde9ae03aL, 0xb9a0a871143b5593L,
    0x587c24548af65815L, 0xe6c28e5be4fddd80L, 0x2a444715176d248dL, 0x81fe44894e733135L,
    0x28830a2804706934L, 0x88cfbf58d59b11baL, 0x26af542cebe6171cL, 0xdde02b2d668c9572L,
    0xb341323be2b6d79eL, 0xcce3c39ab647ab90L, 0xa46c73c76de7613cL, 0x315db06425532036L,
    0x51b5f7b964691a80L, 0xb8121585be436369L, 0x8360b825fe77edabL, 0x0e438d069cee0c4cL,
    0x639a204401fb93d9L, 0x556390b24956e1ebL, 0x9766233775bcc83bL, 0xfa37983741b25870L,
    0x7c1fb605565faf41L, 0xae3654f3c1c2d841L, 0x6df8bd94379a36eaL, 0x2757a960502012bfL,
    0xb121c8166b757838L, 0xbd6b8204adb32b1bL, 0xf5bbe8302088830fL, 0x1db601abef63ebe8L,
    0xcefd52a0e81fc8f6L, 0x6c07851d24f6a181L, 0x583874081de9e873L, 0xb9b6d5b9144dbdafL,
    0xb74d921fcd58059dL, 0x6b95ce183f32c9eaL, 0x5176355a51006a3cL, 0x862cb6ad853fdd61L,
    0x0fb3d2371dc54dffL, 0xc541027d1dd4aa47L, 0x77e05a47127c88d7L, 0x2e6e6ad767d0225fL,
    0xb83710eef530452aL, 0x7e4f8a7fbfb6dcb3L, 0xa9b0629d8812bc93L,
  };

  private static final long[] MIXED_SEED_TERMHASH = {
    0x64ce21f217f362b1L, 0x2a0b0838d1d5334fL, 0xece4aa9da8fb41edL, 0x2e566d4505160f38L,
    0x12a84c4a2b97adb7L, 0xadc12f2f1736749cL, 0x0f84630f126b5abaL, 0xabcc20809f71d579L,
    0xc5287311358e1d9fL, 0x54c7117c7e40363aL, 0x560b653b0b73c545L, 0x7af31a3531753a29L,
    0x26dfc0e097730215L, 0x936359808c12d083L, 0x0b003ca54f195581L, 0xd03aec41fbc61aadL,
    0xe0a01cf72f70db10L, 0x564b34b248903773L, 0x23179cccd8a423fdL, 0x6f172ce1e2aa64afL,
    0xd01cbc6d210d911fL, 0x9eebf2ac93ed7716L, 0x60adee9f73534aa5L, 0x8fe26471f350d778L,
    0xa0e44329ae7e4003L, 0x03fdf459bcb2fef6L, 0x5cfc286015f8bafaL, 0x4cdc50e332887058L,
    0xb82b19a0ada66d96L, 0xb7be6d1d79d675f7L, 0xca6732750206c9e1L, 0x6aff3a06806e6fa5L,
    0x42f10c2842c2de01L, 0x1746dd658f799c71L, 0x1ca74a4e73436dc9L, 0xf33f40b57c7fbd68L,
    0x77978fca66e38726L, 0x13af44687c3057a6L, 0xb790890db3ed6d05L, 0xf0ed04fdbc80741fL,
    0x125acec2094c8868L, 0xe4cdb751633a8c79L, 0x1b66f70f1b2784fcL, 0xfa361571038a4048L,
    0xa8e78f32c59729edL, 0xee1eb8c5f3d6f3f7L, 0xef0dd21edc74d5c6L, 0x854872cc62ddd81eL,
    0x97958711b40dad21L, 0x65b64705f2a6ddd2L, 0x30798013f158e3c8L,
  };

  public void testGoldenValues() {
    final int[] lengths = goldenLengths();
    final byte[] input = goldenInput(lengths[lengths.length - 1]);
    assertGolden(input, lengths, 0, SEED_0);
    assertGolden(input, lengths, TermHash.SEED, SEED_TERMHASH);
  }

  public void testMixedGoldenValues() {
    final byte[] input = mixedInput(MIXED_LENGTHS[MIXED_LENGTHS.length - 1]);
    assertGolden(input, MIXED_LENGTHS, 0, MIXED_SEED_0);
    assertGolden(input, MIXED_LENGTHS, TermHash.SEED, MIXED_SEED_TERMHASH);
  }

  private static void assertGolden(byte[] input, int[] lengths, long seed, long[] expected) {
    assertEquals(lengths.length, expected.length);
    final Xxh3 xxh3 = new Xxh3(seed);
    for (int i = 0; i < lengths.length; i++) {
      assertEquals(
          "seed=" + seed + " length=" + lengths[i], expected[i], xxh3.hash(input, 0, lengths[i]));
    }
  }

  /**
   * The hash of a range depends on its bytes only, wherever they are in the array and whatever
   * surrounds them.
   */
  public void testRanges() {
    final int[] goldenLengths = goldenLengths();
    final byte[] golden = goldenInput(goldenLengths[goldenLengths.length - 1]);
    final byte[] mixed = mixedInput(MIXED_LENGTHS[MIXED_LENGTHS.length - 1]);
    final Xxh3 xxh3 = new Xxh3(TermHash.SEED);
    for (int iter = 0; iter < 500; iter++) {
      final boolean isMixed = random().nextBoolean();
      final int[] lengths = isMixed ? MIXED_LENGTHS : goldenLengths;
      final int index = random().nextInt(lengths.length);
      final int length = lengths[index];
      final int offset = random().nextInt(64);
      final byte[] bytes = new byte[offset + length + random().nextInt(64)];
      random().nextBytes(bytes);
      System.arraycopy(isMixed ? mixed : golden, 0, bytes, offset, length);
      final byte[] copy = bytes.clone();
      final long expected = isMixed ? MIXED_SEED_TERMHASH[index] : SEED_TERMHASH[index];
      assertEquals("length=" + length, expected, xxh3.hash(bytes, offset, length));
      assertArrayEquals(copy, bytes); // not modified
    }
  }

  /** Random inputs: hashing is deterministic, and depends on the seed and on every byte. */
  public void testRandomInputs() {
    final long seed = random().nextLong();
    final Xxh3 xxh3 = new Xxh3(seed);
    final Xxh3 same = new Xxh3(seed);
    final Xxh3 other = new Xxh3(seed + 1);
    for (int iter = 0; iter < 1000; iter++) {
      final int length =
          random().nextBoolean() ? random().nextInt(300) : TestUtil.nextInt(random(), 0, 5000);
      final byte[] bytes = new byte[length];
      random().nextBytes(bytes);
      final long hash = xxh3.hash(bytes, 0, length);
      assertEquals(hash, xxh3.hash(bytes, 0, length));
      assertEquals(hash, same.hash(bytes, 0, length));
      // a 64-bit hash collides with another one with probability 2^-64
      assertNotEquals(hash, other.hash(bytes, 0, length));
      if (length > 0) {
        final int i = random().nextInt(length);
        bytes[i] ^= (byte) (1 << random().nextInt(8));
        assertNotEquals(
            "length=" + length + " flipped byte " + i, hash, xxh3.hash(bytes, 0, length));
      }
    }
  }
}
