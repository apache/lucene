/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

#ifndef __DPU_INSTRUCTION_ENCODER_H
#define __DPU_INSTRUCTION_ENCODER_H

#define REG_D0 (0)
#define REG_D10 (10)
#define REG_D12 (12)
#define REG_D14 (14)
#define REG_D16 (16)
#define REG_D18 (18)
#define REG_D2 (2)
#define REG_D20 (20)
#define REG_D22 (22)
#define REG_D4 (4)
#define REG_D6 (6)
#define REG_D8 (8)
#define REG_ID (28)
#define REG_ID2 (29)
#define REG_ID4 (30)
#define REG_ID8 (31)
#define REG_LNEG (26)
#define REG_MNEG (27)
#define REG_ONE (25)
#define REG_R0 (0)
#define REG_R1 (1)
#define REG_R10 (10)
#define REG_R11 (11)
#define REG_R12 (12)
#define REG_R13 (13)
#define REG_R14 (14)
#define REG_R15 (15)
#define REG_R16 (16)
#define REG_R17 (17)
#define REG_R18 (18)
#define REG_R19 (19)
#define REG_R2 (2)
#define REG_R20 (20)
#define REG_R21 (21)
#define REG_R22 (22)
#define REG_R23 (23)
#define REG_R3 (3)
#define REG_R4 (4)
#define REG_R5 (5)
#define REG_R6 (6)
#define REG_R7 (7)
#define REG_R8 (8)
#define REG_R9 (9)
#define REG_S0 (0)
#define REG_S1 (1)
#define REG_S10 (10)
#define REG_S11 (11)
#define REG_S12 (12)
#define REG_S13 (13)
#define REG_S14 (14)
#define REG_S15 (15)
#define REG_S16 (16)
#define REG_S17 (17)
#define REG_S18 (18)
#define REG_S19 (19)
#define REG_S2 (2)
#define REG_S20 (20)
#define REG_S21 (21)
#define REG_S22 (22)
#define REG_S23 (23)
#define REG_S3 (3)
#define REG_S4 (4)
#define REG_S5 (5)
#define REG_S6 (6)
#define REG_S7 (7)
#define REG_S8 (8)
#define REG_S9 (9)
#define REG_ZERO (24)

#define ACQUIRE_CC(x) ACQUIRE_CC_##x
#define ACQUIRE_CC_NZ (3)
#define ACQUIRE_CC_TRUE (1)
#define ACQUIRE_CC_Z (2)

#define ADD_NZ_CC(x) ADD_NZ_CC_##x
#define ADD_NZ_CC_C (20)
#define ADD_NZ_CC_MI (9)
#define ADD_NZ_CC_NC (21)
#define ADD_NZ_CC_NC10 (27)
#define ADD_NZ_CC_NC11 (28)
#define ADD_NZ_CC_NC12 (29)
#define ADD_NZ_CC_NC13 (30)
#define ADD_NZ_CC_NC14 (31)
#define ADD_NZ_CC_NC5 (22)
#define ADD_NZ_CC_NC6 (23)
#define ADD_NZ_CC_NC7 (24)
#define ADD_NZ_CC_NC8 (25)
#define ADD_NZ_CC_NC9 (26)
#define ADD_NZ_CC_NOV (19)
#define ADD_NZ_CC_NZ (3)
#define ADD_NZ_CC_OV (18)
#define ADD_NZ_CC_PL (8)
#define ADD_NZ_CC_SMI (15)
#define ADD_NZ_CC_SNZ (13)
#define ADD_NZ_CC_SPL (14)
#define ADD_NZ_CC_SZ (12)
#define ADD_NZ_CC_TRUE (1)
#define ADD_NZ_CC_XNZ (5)
#define ADD_NZ_CC_XZ (4)
#define ADD_NZ_CC_Z (2)

#define BOOT_CC(x) BOOT_CC_##x
#define BOOT_CC_FALSE (0)
#define BOOT_CC_NZ (3)
#define BOOT_CC_SMI (15)
#define BOOT_CC_SNZ (13)
#define BOOT_CC_SPL (14)
#define BOOT_CC_SZ (12)
#define BOOT_CC_TRUE (1)
#define BOOT_CC_XNZ (5)
#define BOOT_CC_XZ (4)
#define BOOT_CC_Z (2)

#define CONST_CC_GE0(x) CONST_CC_GE0_##x
#define CONST_CC_GE0_PL (0)

#define CONST_CC_GEU(x) CONST_CC_GEU_##x
#define CONST_CC_GEU_GEU (0)

#define CONST_CC_ZERO(x) CONST_CC_ZERO_##x
#define CONST_CC_ZERO_Z (0)

#define COUNT_NZ_CC(x) COUNT_NZ_CC_##x
#define COUNT_NZ_CC_MAX (8)
#define COUNT_NZ_CC_NMAX (9)
#define COUNT_NZ_CC_NZ (3)
#define COUNT_NZ_CC_SMI (15)
#define COUNT_NZ_CC_SNZ (13)
#define COUNT_NZ_CC_SPL (14)
#define COUNT_NZ_CC_SZ (12)
#define COUNT_NZ_CC_TRUE (1)
#define COUNT_NZ_CC_XNZ (5)
#define COUNT_NZ_CC_XZ (4)
#define COUNT_NZ_CC_Z (2)

#define DIV_CC(x) DIV_CC_##x
#define DIV_CC_FALSE (0)
#define DIV_CC_SMI (15)
#define DIV_CC_SNZ (13)
#define DIV_CC_SPL (14)
#define DIV_CC_SZ (12)
#define DIV_CC_TRUE (1)

#define DIV_NZ_CC(x) DIV_NZ_CC_##x
#define DIV_NZ_CC_SMI (15)
#define DIV_NZ_CC_SNZ (13)
#define DIV_NZ_CC_SPL (14)
#define DIV_NZ_CC_SZ (12)
#define DIV_NZ_CC_TRUE (1)

#define EXT_SUB_SET_CC(x) EXT_SUB_SET_CC_##x
#define EXT_SUB_SET_CC_C (53)
#define EXT_SUB_SET_CC_EQ (6)
#define EXT_SUB_SET_CC_GES (55)
#define EXT_SUB_SET_CC_GEU (53)
#define EXT_SUB_SET_CC_GTS (57)
#define EXT_SUB_SET_CC_GTU (59)
#define EXT_SUB_SET_CC_LES (56)
#define EXT_SUB_SET_CC_LEU (58)
#define EXT_SUB_SET_CC_LTS (54)
#define EXT_SUB_SET_CC_LTU (52)
#define EXT_SUB_SET_CC_MI (41)
#define EXT_SUB_SET_CC_NC (52)
#define EXT_SUB_SET_CC_NEQ (7)
#define EXT_SUB_SET_CC_NOV (51)
#define EXT_SUB_SET_CC_NZ (7)
#define EXT_SUB_SET_CC_OV (50)
#define EXT_SUB_SET_CC_PL (40)
#define EXT_SUB_SET_CC_SMI (47)
#define EXT_SUB_SET_CC_SNZ (45)
#define EXT_SUB_SET_CC_SPL (46)
#define EXT_SUB_SET_CC_SZ (44)
#define EXT_SUB_SET_CC_TRUE (33)
#define EXT_SUB_SET_CC_XGTS (61)
#define EXT_SUB_SET_CC_XGTU (63)
#define EXT_SUB_SET_CC_XLES (60)
#define EXT_SUB_SET_CC_XLEU (62)
#define EXT_SUB_SET_CC_XNZ (11)
#define EXT_SUB_SET_CC_XZ (10)
#define EXT_SUB_SET_CC_Z (6)

#define FALSE_CC(x) FALSE_CC_##x
#define FALSE_CC_FALSE (0)

#define IMM_SHIFT_NZ_CC(x) IMM_SHIFT_NZ_CC_##x
#define IMM_SHIFT_NZ_CC_E (24)
#define IMM_SHIFT_NZ_CC_MI (9)
#define IMM_SHIFT_NZ_CC_NZ (3)
#define IMM_SHIFT_NZ_CC_O (25)
#define IMM_SHIFT_NZ_CC_PL (8)
#define IMM_SHIFT_NZ_CC_SE (30)
#define IMM_SHIFT_NZ_CC_SMI (15)
#define IMM_SHIFT_NZ_CC_SNZ (13)
#define IMM_SHIFT_NZ_CC_SO (31)
#define IMM_SHIFT_NZ_CC_SPL (14)
#define IMM_SHIFT_NZ_CC_SZ (12)
#define IMM_SHIFT_NZ_CC_TRUE (1)
#define IMM_SHIFT_NZ_CC_XNZ (5)
#define IMM_SHIFT_NZ_CC_XZ (4)
#define IMM_SHIFT_NZ_CC_Z (2)

#define LOG_NZ_CC(x) LOG_NZ_CC_##x
#define LOG_NZ_CC_MI (9)
#define LOG_NZ_CC_NZ (3)
#define LOG_NZ_CC_PL (8)
#define LOG_NZ_CC_SMI (15)
#define LOG_NZ_CC_SNZ (13)
#define LOG_NZ_CC_SPL (14)
#define LOG_NZ_CC_SZ (12)
#define LOG_NZ_CC_TRUE (1)
#define LOG_NZ_CC_XNZ (5)
#define LOG_NZ_CC_XZ (4)
#define LOG_NZ_CC_Z (2)

#define LOG_SET_CC(x) LOG_SET_CC_##x
#define LOG_SET_CC_NZ (7)
#define LOG_SET_CC_XNZ (11)
#define LOG_SET_CC_XZ (10)
#define LOG_SET_CC_Z (6)

#define MUL_NZ_CC(x) MUL_NZ_CC_##x
#define MUL_NZ_CC_LARGE (31)
#define MUL_NZ_CC_NZ (3)
#define MUL_NZ_CC_SMALL (30)
#define MUL_NZ_CC_SMI (15)
#define MUL_NZ_CC_SNZ (13)
#define MUL_NZ_CC_SPL (14)
#define MUL_NZ_CC_SZ (12)
#define MUL_NZ_CC_TRUE (1)
#define MUL_NZ_CC_XNZ (5)
#define MUL_NZ_CC_XZ (4)
#define MUL_NZ_CC_Z (2)

#define NO_CC(x) NO_CC_##x

#define RELEASE_CC(x) RELEASE_CC_##x
#define RELEASE_CC_NZ (0)

#define SHIFT_NZ_CC(x) SHIFT_NZ_CC_##x
#define SHIFT_NZ_CC_E (24)
#define SHIFT_NZ_CC_MI (9)
#define SHIFT_NZ_CC_NSH32 (28)
#define SHIFT_NZ_CC_NZ (3)
#define SHIFT_NZ_CC_O (25)
#define SHIFT_NZ_CC_PL (8)
#define SHIFT_NZ_CC_SE (30)
#define SHIFT_NZ_CC_SH32 (29)
#define SHIFT_NZ_CC_SMI (15)
#define SHIFT_NZ_CC_SNZ (13)
#define SHIFT_NZ_CC_SO (31)
#define SHIFT_NZ_CC_SPL (14)
#define SHIFT_NZ_CC_SZ (12)
#define SHIFT_NZ_CC_TRUE (1)
#define SHIFT_NZ_CC_XNZ (5)
#define SHIFT_NZ_CC_XZ (4)
#define SHIFT_NZ_CC_Z (2)

#define SUB_NZ_CC(x) SUB_NZ_CC_##x
#define SUB_NZ_CC_C (21)
#define SUB_NZ_CC_EQ (2)
#define SUB_NZ_CC_GES (23)
#define SUB_NZ_CC_GEU (21)
#define SUB_NZ_CC_GTS (25)
#define SUB_NZ_CC_GTU (27)
#define SUB_NZ_CC_LES (24)
#define SUB_NZ_CC_LEU (26)
#define SUB_NZ_CC_LTS (22)
#define SUB_NZ_CC_LTU (20)
#define SUB_NZ_CC_MI (9)
#define SUB_NZ_CC_NC (20)
#define SUB_NZ_CC_NEQ (3)
#define SUB_NZ_CC_NOV (19)
#define SUB_NZ_CC_NZ (3)
#define SUB_NZ_CC_OV (18)
#define SUB_NZ_CC_PL (8)
#define SUB_NZ_CC_SMI (15)
#define SUB_NZ_CC_SNZ (13)
#define SUB_NZ_CC_SPL (14)
#define SUB_NZ_CC_SZ (12)
#define SUB_NZ_CC_TRUE (1)
#define SUB_NZ_CC_XGTS (29)
#define SUB_NZ_CC_XGTU (31)
#define SUB_NZ_CC_XLES (28)
#define SUB_NZ_CC_XLEU (30)
#define SUB_NZ_CC_XNZ (5)
#define SUB_NZ_CC_XZ (4)
#define SUB_NZ_CC_Z (2)

#define SUB_SET_CC(x) SUB_SET_CC_##x
#define SUB_SET_CC_EQ (6)
#define SUB_SET_CC_NEQ (7)
#define SUB_SET_CC_NZ (7)
#define SUB_SET_CC_XNZ (11)
#define SUB_SET_CC_XZ (10)
#define SUB_SET_CC_Z (6)

#define TRUE_CC(x) TRUE_CC_##x
#define TRUE_CC_TRUE (1)

#define TRUE_FALSE_CC(x) TRUE_FALSE_CC_##x
#define TRUE_FALSE_CC_FALSE (0)
#define TRUE_FALSE_CC_TRUE (1)

#define AT_PC(x) (x)

#define ACQUIRErici(ra, imm, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x7c0380000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1f) << 26) | (((((dpuinstruction_t)imm) >> 13) & 0x7) << 39)                      \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0) | (((((dpuinstruction_t)cc) >> 0) & 0x3) << 24)))
#define ADD_Srri(dc, rb, imm)                                                                                                    \
    ((dpuinstruction_t)(0xe000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                            \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34)                        \
        | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 44) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define ADD_Srric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c8300000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADD_Srrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c8300000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADD_Srrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x4c8300000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADD_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x4c8000c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ADD_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x4c8000c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define ADD_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x4c8000c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADD_Urri(dc, rb, imm)                                                                                                    \
    ((dpuinstruction_t)(0x6000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                            \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34)                        \
        | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 44) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define ADD_Urric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c0300000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADD_Urrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c0300000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADD_Urrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x4c0300000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADD_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x4c0000c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ADD_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x4c0000c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define ADD_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x4c0000c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDrri(rc, ra, imm)                                                                                                      \
    ((dpuinstruction_t)(0x300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                            \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define ADDrric(rc, ra, imm, cc)                                                                                                 \
    ((dpuinstruction_t)(0xc0300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADDrrici(rc, ra, imm, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0xc0300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDrrif(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0xc0300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADDrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0xc0000c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ADDrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0xc0000c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define ADDrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0xc0000c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDssi(rc, ra, imm)                                                                                                      \
    ((dpuinstruction_t)(0xc0310000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0x1f) << 0) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 5)                       \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 6) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 7)                        \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 8) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 9)                        \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 10) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 11)))
#define ADDsss(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0xc0010c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x7) << 17) | (((((dpuinstruction_t)ra) >> 3) & 0x3) << 32)))
#define ADDzri(rb, imm)                                                                                                          \
    ((dpuinstruction_t)(0x606000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34) | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define ADDzric(ra, imm, cc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define ADDzrici(ra, imm, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x3c0300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDzrif(ra, imm)                                                                                                         \
    ((dpuinstruction_t)(0x3c0300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define ADDzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x3c0000c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ADDzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x3c0000c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define ADDzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x3c0000c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDC_Srric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x4c8320000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADDC_Srrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x4c8320000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDC_Srrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x4c8320000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADDC_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x4c8020c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ADDC_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c8020c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define ADDC_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c8020c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDC_Urric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x4c0320000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADDC_Urrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x4c0320000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDC_Urrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x4c0320000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADDC_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x4c0020c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ADDC_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c0020c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define ADDC_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c0020c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDCrri(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0x100300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define ADDCrric(rc, ra, imm, cc)                                                                                                \
    ((dpuinstruction_t)(0xc0320000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADDCrrici(rc, ra, imm, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0xc0320000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDCrrif(rc, ra, imm)                                                                                                    \
    ((dpuinstruction_t)(0xc0320000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ADDCrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0xc0020c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ADDCrrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0xc0020c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define ADDCrrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0xc0020c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDCzri(rb, imm)                                                                                                         \
    ((dpuinstruction_t)(0x616000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34) | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define ADDCzric(ra, imm, cc)                                                                                                    \
    ((dpuinstruction_t)(0x3c0320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define ADDCzrici(ra, imm, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x3c0320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDCzrif(ra, imm)                                                                                                        \
    ((dpuinstruction_t)(0x3c0320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define ADDCzrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x3c0020c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ADDCzrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0020c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define ADDCzrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x3c0020c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ADDSrri(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0xc0310000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0x1f) << 0) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 5)                       \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 6) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 7)                        \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 8) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 9)                        \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 10) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 11)))
#define ADDSrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0xc0010c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x7) << 17) | (((((dpuinstruction_t)ra) >> 3) & 0x3) << 32)))
#define AND_Srki(dc, ra, imm)                                                                                                    \
    ((dpuinstruction_t)(0x508300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define AND_Srri(dc, rb, imm)                                                                                                    \
    ((dpuinstruction_t)(0x20e000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34)                        \
        | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 44) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define AND_Srric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x9083a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define AND_Srrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9083a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define AND_Srrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x9083a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define AND_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9080c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define AND_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9080c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define AND_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9080c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define AND_Urki(dc, ra, imm)                                                                                                    \
    ((dpuinstruction_t)(0x500300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define AND_Urri(dc, rb, imm)                                                                                                    \
    ((dpuinstruction_t)(0x206000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34)                        \
        | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 44) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define AND_Urric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x9003a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define AND_Urrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9003a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define AND_Urrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x9003a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define AND_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9000c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define AND_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9000c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define AND_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9000c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDrri(rc, ra, imm)                                                                                                      \
    ((dpuinstruction_t)(0x500300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define ANDrric(rc, ra, imm, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8003a0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ANDrrici(rc, ra, imm, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8003a0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDrrif(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0x8003a0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ANDrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0x8000c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ANDrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8000c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ANDrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8000c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDzri(rb, imm)                                                                                                          \
    ((dpuinstruction_t)(0x656000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34) | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define ANDzric(ra, imm, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c03a0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39) | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define ANDzrici(ra, imm, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c03a0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 44) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDzrif(ra, imm)                                                                                                         \
    ((dpuinstruction_t)(0x8c03a0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define ANDzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x8c00c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ANDzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x8c00c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ANDzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8c00c0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDN_Srric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x9083c0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ANDN_Srrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x9083c0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDN_Srrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x9083c0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ANDN_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9080c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ANDN_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9080c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ANDN_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9080c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDN_Urric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x9003c0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ANDN_Urrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x9003c0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDN_Urrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x9003c0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ANDN_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9000c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ANDN_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9000c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ANDN_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9000c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDNrri(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0x8003c0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ANDNrric(rc, ra, imm, cc)                                                                                                \
    ((dpuinstruction_t)(0x8003c0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ANDNrrici(rc, ra, imm, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x8003c0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDNrrif(rc, ra, imm)                                                                                                    \
    ((dpuinstruction_t)(0x8003c0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ANDNrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x8000c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ANDNrrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8000c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ANDNrrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8000c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDNzric(ra, imm, cc)                                                                                                    \
    ((dpuinstruction_t)(0x8c03c0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39) | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define ANDNzrici(ra, imm, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x8c03c0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 44) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ANDNzrif(ra, imm)                                                                                                        \
    ((dpuinstruction_t)(0x8c03c0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define ANDNzrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x8c00c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ANDNzrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c00c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ANDNzrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c00c0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ASR_Srri(dc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x908340080000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ASR_Srric(dc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x908340080000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ASR_Srrici(dc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x908340080000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ASR_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9080d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ASR_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9080d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ASR_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9080d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ASR_Urri(dc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x900340080000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ASR_Urric(dc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x900340080000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ASR_Urrici(dc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x900340080000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ASR_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9000d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ASR_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9000d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ASR_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9000d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ASRrri(rc, ra, shift)                                                                                                    \
    ((dpuinstruction_t)(0x800340080000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ASRrric(rc, ra, shift, cc)                                                                                               \
    ((dpuinstruction_t)(0x800340080000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ASRrrici(rc, ra, shift, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x800340080000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ASRrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0x8000d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ASRrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8000d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ASRrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8000d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ASRzri(ra, shift)                                                                                                        \
    ((dpuinstruction_t)(0x8c0340080000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ASRzric(ra, shift, cc)                                                                                                   \
    ((dpuinstruction_t)(0x8c0340080000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ASRzrici(ra, shift, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x8c0340080000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ASRzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x8c00d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ASRzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x8c00d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ASRzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8c00d0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define BKP() ((dpuinstruction_t)(0x7e6320000000l))
#define BOOTri(ra, imm)                                                                                                          \
    ((dpuinstruction_t)(0x7d8320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)))
#define BOOTrici(ra, imm, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x7d8320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CALLri(rc, off)                                                                                                          \
    ((dpuinstruction_t)(0x806300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define CALLrr(rc, ra)                                                                                                           \
    ((dpuinstruction_t)(0x800300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CALLrri(rc, ra, off)                                                                                                     \
    ((dpuinstruction_t)(0x800300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define CALLrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x8000c0000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CALLzri(ra, off)                                                                                                         \
    ((dpuinstruction_t)(0x8c0300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)off) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)off) >> 27) & 0x1) << 44)))
#define CALLzrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x8c00c0000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CAO_Srr(dc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x908330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define CAO_Srrc(dc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x908330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CAO_Srrci(dc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x908330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CAO_Urr(dc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x900330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define CAO_Urrc(dc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x900330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CAO_Urrci(dc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x900330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CAOrr(rc, ra)                                                                                                            \
    ((dpuinstruction_t)(0x800330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define CAOrrc(rc, ra, cc)                                                                                                       \
    ((dpuinstruction_t)(0x800330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CAOrrci(rc, ra, cc, pc)                                                                                                  \
    ((dpuinstruction_t)(0x800330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CAOzr(ra) ((dpuinstruction_t)(0x8c0330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CAOzrc(ra, cc)                                                                                                           \
    ((dpuinstruction_t)(0x8c0330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CAOzrci(ra, cc, pc)                                                                                                      \
    ((dpuinstruction_t)(0x8c0330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLO_Srr(dc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x908330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define CLO_Srrc(dc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x908330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLO_Srrci(dc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x908330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLO_Urr(dc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x900330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define CLO_Urrc(dc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x900330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLO_Urrci(dc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x900330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLOrr(rc, ra)                                                                                                            \
    ((dpuinstruction_t)(0x800330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define CLOrrc(rc, ra, cc)                                                                                                       \
    ((dpuinstruction_t)(0x800330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLOrrci(rc, ra, cc, pc)                                                                                                  \
    ((dpuinstruction_t)(0x800330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLOzr(ra) ((dpuinstruction_t)(0x8c0330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CLOzrc(ra, cc)                                                                                                           \
    ((dpuinstruction_t)(0x8c0330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLOzrci(ra, cc, pc)                                                                                                      \
    ((dpuinstruction_t)(0x8c0330100000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLR_RUNrici(ra, imm, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x7c8320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLS_Srr(dc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x908330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define CLS_Srrc(dc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x908330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLS_Srrci(dc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x908330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLS_Urr(dc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x900330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define CLS_Urrc(dc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x900330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLS_Urrci(dc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x900330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLSrr(rc, ra)                                                                                                            \
    ((dpuinstruction_t)(0x800330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define CLSrrc(rc, ra, cc)                                                                                                       \
    ((dpuinstruction_t)(0x800330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLSrrci(rc, ra, cc, pc)                                                                                                  \
    ((dpuinstruction_t)(0x800330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLSzr(ra) ((dpuinstruction_t)(0x8c0330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CLSzrc(ra, cc)                                                                                                           \
    ((dpuinstruction_t)(0x8c0330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLSzrci(ra, cc, pc)                                                                                                      \
    ((dpuinstruction_t)(0x8c0330200000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLZ_Srr(dc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x908330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define CLZ_Srrc(dc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x908330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLZ_Srrci(dc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x908330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLZ_Urr(dc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x900330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define CLZ_Urrc(dc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x900330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLZ_Urrci(dc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x900330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLZrr(rc, ra)                                                                                                            \
    ((dpuinstruction_t)(0x800330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define CLZrrc(rc, ra, cc)                                                                                                       \
    ((dpuinstruction_t)(0x800330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLZrrci(rc, ra, cc, pc)                                                                                                  \
    ((dpuinstruction_t)(0x800330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CLZzr(ra) ((dpuinstruction_t)(0x8c0330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CLZzrc(ra, cc)                                                                                                           \
    ((dpuinstruction_t)(0x8c0330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CLZzrci(ra, cc, pc)                                                                                                      \
    ((dpuinstruction_t)(0x8c0330300000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CMPB4_Srrr(dc, ra, rb)                                                                                                   \
    ((dpuinstruction_t)(0x908080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CMPB4_Srrrc(dc, ra, rb, cc)                                                                                              \
    ((dpuinstruction_t)(0x908080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CMPB4_Srrrci(dc, ra, rb, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x908080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CMPB4_Urrr(dc, ra, rb)                                                                                                   \
    ((dpuinstruction_t)(0x900080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CMPB4_Urrrc(dc, ra, rb, cc)                                                                                              \
    ((dpuinstruction_t)(0x900080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CMPB4_Urrrci(dc, ra, rb, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x900080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CMPB4rrr(rc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x800080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CMPB4rrrc(rc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x800080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CMPB4rrrci(rc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x800080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define CMPB4zrr(ra, rb)                                                                                                         \
    ((dpuinstruction_t)(0x8c0080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define CMPB4zrrc(ra, rb, cc)                                                                                                    \
    ((dpuinstruction_t)(0x8c0080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define CMPB4zrrci(ra, rb, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x8c0080000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define DIV_STEPrrri(dc, ra, db, shift)                                                                                          \
    ((dpuinstruction_t)(0x800060010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define DIV_STEPrrrici(dc, ra, db, shift, cc, pc)                                                                                \
    ((dpuinstruction_t)(0x800060010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTSB_Srr(dc, ra)                                                                                                        \
    ((dpuinstruction_t)(0x908330500000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define EXTSB_Srrc(dc, ra, cc)                                                                                                   \
    ((dpuinstruction_t)(0x908330500000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTSB_Srrci(dc, ra, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x908330500000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTSBrr(rc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x800330500000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define EXTSBrrc(rc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x800330500000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTSBrrci(rc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x800330500000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTSBzr(ra) ((dpuinstruction_t)(0x8c0330500000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define EXTSBzrc(ra, cc)                                                                                                         \
    ((dpuinstruction_t)(0x8c0330500000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTSBzrci(ra, cc, pc)                                                                                                    \
    ((dpuinstruction_t)(0x8c0330500000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTSH_Srr(dc, ra)                                                                                                        \
    ((dpuinstruction_t)(0x908330700000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define EXTSH_Srrc(dc, ra, cc)                                                                                                   \
    ((dpuinstruction_t)(0x908330700000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTSH_Srrci(dc, ra, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x908330700000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTSHrr(rc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x800330700000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define EXTSHrrc(rc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x800330700000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTSHrrci(rc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x800330700000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTSHzr(ra) ((dpuinstruction_t)(0x8c0330700000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define EXTSHzrc(ra, cc)                                                                                                         \
    ((dpuinstruction_t)(0x8c0330700000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTSHzrci(ra, cc, pc)                                                                                                    \
    ((dpuinstruction_t)(0x8c0330700000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTUB_Urr(dc, ra)                                                                                                        \
    ((dpuinstruction_t)(0x900330400000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define EXTUB_Urrc(dc, ra, cc)                                                                                                   \
    ((dpuinstruction_t)(0x900330400000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTUB_Urrci(dc, ra, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x900330400000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTUBrr(rc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x800330400000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define EXTUBrrc(rc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x800330400000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTUBrrci(rc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x800330400000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTUBzr(ra) ((dpuinstruction_t)(0x8c0330400000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define EXTUBzrc(ra, cc)                                                                                                         \
    ((dpuinstruction_t)(0x8c0330400000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTUBzrci(ra, cc, pc)                                                                                                    \
    ((dpuinstruction_t)(0x8c0330400000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTUH_Urr(dc, ra)                                                                                                        \
    ((dpuinstruction_t)(0x900330600000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define EXTUH_Urrc(dc, ra, cc)                                                                                                   \
    ((dpuinstruction_t)(0x900330600000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTUH_Urrci(dc, ra, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x900330600000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTUHrr(rc, ra)                                                                                                          \
    ((dpuinstruction_t)(0x800330600000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define EXTUHrrc(rc, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x800330600000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTUHrrci(rc, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x800330600000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define EXTUHzr(ra) ((dpuinstruction_t)(0x8c0330600000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define EXTUHzrc(ra, cc)                                                                                                         \
    ((dpuinstruction_t)(0x8c0330600000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define EXTUHzrci(ra, cc, pc)                                                                                                    \
    ((dpuinstruction_t)(0x8c0330600000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define FAULTi(imm)                                                                                                              \
    ((dpuinstruction_t)(0x7e6320000000l | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define HASH_Srric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x908310000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define HASH_Srrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x908310000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define HASH_Srrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x908310000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define HASH_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9080c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define HASH_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9080c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define HASH_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9080c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define HASH_Urric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x900310000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define HASH_Urrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x900310000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define HASH_Urrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x900310000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define HASH_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9000c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define HASH_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9000c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define HASH_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9000c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define HASHrri(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0x800310000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define HASHrric(rc, ra, imm, cc)                                                                                                \
    ((dpuinstruction_t)(0x800310000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define HASHrrici(rc, ra, imm, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x800310000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define HASHrrif(rc, ra, imm)                                                                                                    \
    ((dpuinstruction_t)(0x800310000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define HASHrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x8000c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define HASHrrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8000c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define HASHrrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8000c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define HASHzric(ra, imm, cc)                                                                                                    \
    ((dpuinstruction_t)(0x8c0310000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39) | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define HASHzrici(ra, imm, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x8c0310000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 44) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define HASHzrif(ra, imm)                                                                                                        \
    ((dpuinstruction_t)(0x8c0310000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define HASHzrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x8c00c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define HASHzrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c00c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define HASHzrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c00c0100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JEQrii(ra, imm, pc)                                                                                                      \
    ((dpuinstruction_t)(0x3c0382000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JEQrri(ra, rb, pc)                                                                                                       \
    ((dpuinstruction_t)(0x3c0082c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JGESrii(ra, imm, pc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0397000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JGESrri(ra, rb, pc)                                                                                                      \
    ((dpuinstruction_t)(0x3c0097c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JGEUrii(ra, imm, pc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0395000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JGEUrri(ra, rb, pc)                                                                                                      \
    ((dpuinstruction_t)(0x3c0095c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JGTSrii(ra, imm, pc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0399000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JGTSrri(ra, rb, pc)                                                                                                      \
    ((dpuinstruction_t)(0x3c0099c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JGTUrii(ra, imm, pc)                                                                                                     \
    ((dpuinstruction_t)(0x3c039b000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JGTUrri(ra, rb, pc)                                                                                                      \
    ((dpuinstruction_t)(0x3c009bc00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JLESrii(ra, imm, pc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0398000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JLESrri(ra, rb, pc)                                                                                                      \
    ((dpuinstruction_t)(0x3c0098c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JLEUrii(ra, imm, pc)                                                                                                     \
    ((dpuinstruction_t)(0x3c039a000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JLEUrri(ra, rb, pc)                                                                                                      \
    ((dpuinstruction_t)(0x3c009ac00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JLTSrii(ra, imm, pc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0396000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JLTSrri(ra, rb, pc)                                                                                                      \
    ((dpuinstruction_t)(0x3c0096c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JLTUrii(ra, imm, pc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0394000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JLTUrri(ra, rb, pc)                                                                                                      \
    ((dpuinstruction_t)(0x3c0094c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JNEQrii(ra, imm, pc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0383000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JNEQrri(ra, rb, pc)                                                                                                      \
    ((dpuinstruction_t)(0x3c0083c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JNZri(ra, pc)                                                                                                            \
    ((dpuinstruction_t)(0x3c0383000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define JUMPi(off)                                                                                                               \
    ((dpuinstruction_t)(0x8c6300000000l | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                                         \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)off) >> 24) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 27) & 0x1) << 44)))
#define JUMPr(ra) ((dpuinstruction_t)(0x8c0300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define JUMPri(ra, off)                                                                                                          \
    ((dpuinstruction_t)(0x8c0300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)off) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)off) >> 27) & 0x1) << 44)))
#define JZri(ra, pc)                                                                                                             \
    ((dpuinstruction_t)(0x3c0382000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LBS_Serri(endian, dc, ra, off)                                                                                           \
    ((dpuinstruction_t)(0x708361000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LBS_Srri(dc, ra, off)                                                                                                    \
    ((dpuinstruction_t)(0x708361000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LBSerri(endian, rc, ra, off)                                                                                             \
    ((dpuinstruction_t)(0x700341000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LBSersi(endian, rc, ra, off)                                                                                             \
    ((dpuinstruction_t)(0x700351000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LBSrri(rc, ra, off)                                                                                                      \
    ((dpuinstruction_t)(0x700341000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LBSSrri(rc, ra, off)                                                                                                     \
    ((dpuinstruction_t)(0x700351000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LBU_Uerri(endian, dc, ra, off)                                                                                           \
    ((dpuinstruction_t)(0x700360000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LBU_Urri(dc, ra, off)                                                                                                    \
    ((dpuinstruction_t)(0x700360000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LBUerri(endian, rc, ra, off)                                                                                             \
    ((dpuinstruction_t)(0x700340000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LBUersi(endian, rc, ra, off)                                                                                             \
    ((dpuinstruction_t)(0x700350000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LBUrri(rc, ra, off)                                                                                                      \
    ((dpuinstruction_t)(0x700340000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LBUSrri(rc, ra, off)                                                                                                     \
    ((dpuinstruction_t)(0x700350000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LDerri(endian, dc, ra, off)                                                                                              \
    ((dpuinstruction_t)(0x700346000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)))
#define LDersi(endian, dc, ra, off)                                                                                              \
    ((dpuinstruction_t)(0x700356000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)))
#define LDrri(dc, ra, off)                                                                                                       \
    ((dpuinstruction_t)(0x700346000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)))
#define LDMArri(ra, rb, immDma)                                                                                                  \
    ((dpuinstruction_t)(0x700000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)immDma) >> 0) & 0xff) << 24)))
#define LDMAIrri(ra, rb, immDma)                                                                                                 \
    ((dpuinstruction_t)(0x700000000001l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)immDma) >> 0) & 0xff) << 24)))
#define LDSrri(dc, ra, off)                                                                                                      \
    ((dpuinstruction_t)(0x700356000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)))
#define LHS_Serri(endian, dc, ra, off)                                                                                           \
    ((dpuinstruction_t)(0x708363000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LHS_Srri(dc, ra, off)                                                                                                    \
    ((dpuinstruction_t)(0x708363000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LHSerri(endian, rc, ra, off)                                                                                             \
    ((dpuinstruction_t)(0x700343000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LHSersi(endian, rc, ra, off)                                                                                             \
    ((dpuinstruction_t)(0x700353000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LHSrri(rc, ra, off)                                                                                                      \
    ((dpuinstruction_t)(0x700343000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LHSSrri(rc, ra, off)                                                                                                     \
    ((dpuinstruction_t)(0x700353000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LHU_Uerri(endian, dc, ra, off)                                                                                           \
    ((dpuinstruction_t)(0x700362000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LHU_Urri(dc, ra, off)                                                                                                    \
    ((dpuinstruction_t)(0x700362000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LHUerri(endian, rc, ra, off)                                                                                             \
    ((dpuinstruction_t)(0x700342000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LHUersi(endian, rc, ra, off)                                                                                             \
    ((dpuinstruction_t)(0x700352000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LHUrri(rc, ra, off)                                                                                                      \
    ((dpuinstruction_t)(0x700342000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LHUSrri(rc, ra, off)                                                                                                     \
    ((dpuinstruction_t)(0x700352000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LSL_Srri(dc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x908340040000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_Srric(dc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x908340040000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_Srrici(dc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x908340040000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9080d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSL_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9080d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSL_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9080d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_Urri(dc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x900340040000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_Urric(dc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x900340040000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_Urrici(dc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x900340040000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9000d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSL_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9000d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSL_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9000d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1_Srri(dc, ra, shift)                                                                                                 \
    ((dpuinstruction_t)(0x908340060000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1_Srric(dc, ra, shift, cc)                                                                                            \
    ((dpuinstruction_t)(0x908340060000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1_Srrici(dc, ra, shift, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0x908340060000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9080d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSL1_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9080d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSL1_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9080d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1_Urri(dc, ra, shift)                                                                                                 \
    ((dpuinstruction_t)(0x900340060000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1_Urric(dc, ra, shift, cc)                                                                                            \
    ((dpuinstruction_t)(0x900340060000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1_Urrici(dc, ra, shift, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0x900340060000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9000d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSL1_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9000d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSL1_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9000d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1rri(rc, ra, shift)                                                                                                   \
    ((dpuinstruction_t)(0x800340060000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1rric(rc, ra, shift, cc)                                                                                              \
    ((dpuinstruction_t)(0x800340060000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1rrici(rc, ra, shift, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x800340060000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1rrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x8000d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSL1rrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8000d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSL1rrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8000d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1zri(ra, shift)                                                                                                       \
    ((dpuinstruction_t)(0x8c0340060000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1zric(ra, shift, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8c0340060000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1zrici(ra, shift, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8c0340060000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1zrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x8c00d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSL1zrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c00d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSL1zrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c00d0600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1X_Srri(dc, ra, shift)                                                                                                \
    ((dpuinstruction_t)(0x908340070000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1X_Srric(dc, ra, shift, cc)                                                                                           \
    ((dpuinstruction_t)(0x908340070000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1X_Srrici(dc, ra, shift, cc, pc)                                                                                      \
    ((dpuinstruction_t)(0x908340070000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1X_Srrr(dc, ra, rb)                                                                                                   \
    ((dpuinstruction_t)(0x9080d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSL1X_Srrrc(dc, ra, rb, cc)                                                                                              \
    ((dpuinstruction_t)(0x9080d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSL1X_Srrrci(dc, ra, rb, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x9080d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1X_Urri(dc, ra, shift)                                                                                                \
    ((dpuinstruction_t)(0x900340070000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1X_Urric(dc, ra, shift, cc)                                                                                           \
    ((dpuinstruction_t)(0x900340070000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1X_Urrici(dc, ra, shift, cc, pc)                                                                                      \
    ((dpuinstruction_t)(0x900340070000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1X_Urrr(dc, ra, rb)                                                                                                   \
    ((dpuinstruction_t)(0x9000d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSL1X_Urrrc(dc, ra, rb, cc)                                                                                              \
    ((dpuinstruction_t)(0x9000d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSL1X_Urrrci(dc, ra, rb, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x9000d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1Xrri(rc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x800340070000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1Xrric(rc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x800340070000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1Xrrici(rc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x800340070000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1Xrrr(rc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x8000d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSL1Xrrrc(rc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x8000d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSL1Xrrrci(rc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x8000d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1Xzri(ra, shift)                                                                                                      \
    ((dpuinstruction_t)(0x8c0340070000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1Xzric(ra, shift, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8c0340070000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL1Xzrici(ra, shift, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8c0340070000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL1Xzrr(ra, rb)                                                                                                         \
    ((dpuinstruction_t)(0x8c00d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSL1Xzrrc(ra, rb, cc)                                                                                                    \
    ((dpuinstruction_t)(0x8c00d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSL1Xzrrci(ra, rb, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x8c00d0700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLrri(rc, ra, shift)                                                                                                    \
    ((dpuinstruction_t)(0x800340040000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLrric(rc, ra, shift, cc)                                                                                               \
    ((dpuinstruction_t)(0x800340040000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLrrici(rc, ra, shift, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x800340040000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0x8000d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSLrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8000d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSLrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8000d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLzri(ra, shift)                                                                                                        \
    ((dpuinstruction_t)(0x8c0340040000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLzric(ra, shift, cc)                                                                                                   \
    ((dpuinstruction_t)(0x8c0340040000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLzrici(ra, shift, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x8c0340040000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x8c00d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSLzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x8c00d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSLzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8c00d0400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_ADD_Srrri(dc, rb, ra, shift)                                                                                         \
    ((dpuinstruction_t)(0x908040000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_ADD_Srrrici(dc, rb, ra, shift, cc, pc)                                                                               \
    ((dpuinstruction_t)(0x908040000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_ADD_Urrri(dc, rb, ra, shift)                                                                                         \
    ((dpuinstruction_t)(0x900040000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_ADD_Urrrici(dc, rb, ra, shift, cc, pc)                                                                               \
    ((dpuinstruction_t)(0x900040000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_ADDrrri(rc, rb, ra, shift)                                                                                           \
    ((dpuinstruction_t)(0x800040000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_ADDrrrici(rc, rb, ra, shift, cc, pc)                                                                                 \
    ((dpuinstruction_t)(0x800040000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_ADDzrri(rb, ra, shift)                                                                                               \
    ((dpuinstruction_t)(0x8c0040000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_ADDzrrici(rb, ra, shift, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x8c0040000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_SUB_Srrri(dc, rb, ra, shift)                                                                                         \
    ((dpuinstruction_t)(0x908060000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_SUB_Srrrici(dc, rb, ra, shift, cc, pc)                                                                               \
    ((dpuinstruction_t)(0x908060000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_SUB_Urrri(dc, rb, ra, shift)                                                                                         \
    ((dpuinstruction_t)(0x900060000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_SUB_Urrrici(dc, rb, ra, shift, cc, pc)                                                                               \
    ((dpuinstruction_t)(0x900060000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_SUBrrri(rc, rb, ra, shift)                                                                                           \
    ((dpuinstruction_t)(0x800060000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_SUBrrrici(rc, rb, ra, shift, cc, pc)                                                                                 \
    ((dpuinstruction_t)(0x800060000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSL_SUBzrri(rb, ra, shift)                                                                                               \
    ((dpuinstruction_t)(0x8c0060000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSL_SUBzrrici(rb, ra, shift, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x8c0060000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLX_Srri(dc, ra, shift)                                                                                                 \
    ((dpuinstruction_t)(0x908340050000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLX_Srric(dc, ra, shift, cc)                                                                                            \
    ((dpuinstruction_t)(0x908340050000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLX_Srrici(dc, ra, shift, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0x908340050000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLX_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9080d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSLX_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9080d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSLX_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9080d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLX_Urri(dc, ra, shift)                                                                                                 \
    ((dpuinstruction_t)(0x900340050000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLX_Urric(dc, ra, shift, cc)                                                                                            \
    ((dpuinstruction_t)(0x900340050000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLX_Urrici(dc, ra, shift, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0x900340050000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLX_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9000d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSLX_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9000d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSLX_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9000d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLXrri(rc, ra, shift)                                                                                                   \
    ((dpuinstruction_t)(0x800340050000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLXrric(rc, ra, shift, cc)                                                                                              \
    ((dpuinstruction_t)(0x800340050000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLXrrici(rc, ra, shift, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x800340050000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLXrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x8000d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSLXrrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8000d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSLXrrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8000d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLXzri(ra, shift)                                                                                                       \
    ((dpuinstruction_t)(0x8c0340050000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLXzric(ra, shift, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8c0340050000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSLXzrici(ra, shift, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8c0340050000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSLXzrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x8c00d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSLXzrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c00d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSLXzrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c00d0500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR_Srri(dc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x9083400c0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR_Srric(dc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x9083400c0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR_Srrici(dc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x9083400c0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9080d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSR_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9080d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSR_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9080d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR_Urri(dc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x9003400c0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR_Urric(dc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x9003400c0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR_Urrici(dc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x9003400c0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9000d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSR_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9000d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSR_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9000d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1_Srri(dc, ra, shift)                                                                                                 \
    ((dpuinstruction_t)(0x9083400e0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1_Srric(dc, ra, shift, cc)                                                                                            \
    ((dpuinstruction_t)(0x9083400e0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1_Srrici(dc, ra, shift, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0x9083400e0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9080d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSR1_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9080d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSR1_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9080d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1_Urri(dc, ra, shift)                                                                                                 \
    ((dpuinstruction_t)(0x9003400e0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1_Urric(dc, ra, shift, cc)                                                                                            \
    ((dpuinstruction_t)(0x9003400e0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1_Urrici(dc, ra, shift, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0x9003400e0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9000d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSR1_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9000d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSR1_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9000d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1rri(rc, ra, shift)                                                                                                   \
    ((dpuinstruction_t)(0x8003400e0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1rric(rc, ra, shift, cc)                                                                                              \
    ((dpuinstruction_t)(0x8003400e0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1rrici(rc, ra, shift, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x8003400e0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1rrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x8000d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSR1rrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8000d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSR1rrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8000d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1zri(ra, shift)                                                                                                       \
    ((dpuinstruction_t)(0x8c03400e0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1zric(ra, shift, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8c03400e0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1zrici(ra, shift, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8c03400e0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1zrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x8c00d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSR1zrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c00d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSR1zrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c00d0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1X_Srri(dc, ra, shift)                                                                                                \
    ((dpuinstruction_t)(0x9083400f0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1X_Srric(dc, ra, shift, cc)                                                                                           \
    ((dpuinstruction_t)(0x9083400f0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1X_Srrici(dc, ra, shift, cc, pc)                                                                                      \
    ((dpuinstruction_t)(0x9083400f0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1X_Srrr(dc, ra, rb)                                                                                                   \
    ((dpuinstruction_t)(0x9080d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSR1X_Srrrc(dc, ra, rb, cc)                                                                                              \
    ((dpuinstruction_t)(0x9080d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSR1X_Srrrci(dc, ra, rb, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x9080d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1X_Urri(dc, ra, shift)                                                                                                \
    ((dpuinstruction_t)(0x9003400f0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1X_Urric(dc, ra, shift, cc)                                                                                           \
    ((dpuinstruction_t)(0x9003400f0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1X_Urrici(dc, ra, shift, cc, pc)                                                                                      \
    ((dpuinstruction_t)(0x9003400f0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1X_Urrr(dc, ra, rb)                                                                                                   \
    ((dpuinstruction_t)(0x9000d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSR1X_Urrrc(dc, ra, rb, cc)                                                                                              \
    ((dpuinstruction_t)(0x9000d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSR1X_Urrrci(dc, ra, rb, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x9000d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1Xrri(rc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x8003400f0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1Xrric(rc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x8003400f0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1Xrrici(rc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x8003400f0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1Xrrr(rc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x8000d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSR1Xrrrc(rc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x8000d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSR1Xrrrci(rc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x8000d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1Xzri(ra, shift)                                                                                                      \
    ((dpuinstruction_t)(0x8c03400f0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1Xzric(ra, shift, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8c03400f0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR1Xzrici(ra, shift, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8c03400f0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR1Xzrr(ra, rb)                                                                                                         \
    ((dpuinstruction_t)(0x8c00d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSR1Xzrrc(ra, rb, cc)                                                                                                    \
    ((dpuinstruction_t)(0x8c00d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSR1Xzrrci(ra, rb, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x8c00d0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRrri(rc, ra, shift)                                                                                                    \
    ((dpuinstruction_t)(0x8003400c0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRrric(rc, ra, shift, cc)                                                                                               \
    ((dpuinstruction_t)(0x8003400c0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRrrici(rc, ra, shift, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x8003400c0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0x8000d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSRrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8000d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSRrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8000d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRzri(ra, shift)                                                                                                        \
    ((dpuinstruction_t)(0x8c03400c0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRzric(ra, shift, cc)                                                                                                   \
    ((dpuinstruction_t)(0x8c03400c0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRzrici(ra, shift, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x8c03400c0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x8c00d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSRzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x8c00d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSRzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8c00d0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR_ADD_Srrri(dc, rb, ra, shift)                                                                                         \
    ((dpuinstruction_t)(0x908020000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR_ADD_Srrrici(dc, rb, ra, shift, cc, pc)                                                                               \
    ((dpuinstruction_t)(0x908020000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR_ADD_Urrri(dc, rb, ra, shift)                                                                                         \
    ((dpuinstruction_t)(0x900020000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR_ADD_Urrrici(dc, rb, ra, shift, cc, pc)                                                                               \
    ((dpuinstruction_t)(0x900020000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR_ADDrrri(rc, rb, ra, shift)                                                                                           \
    ((dpuinstruction_t)(0x800020000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR_ADDrrrici(rc, rb, ra, shift, cc, pc)                                                                                 \
    ((dpuinstruction_t)(0x800020000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSR_ADDzrri(rb, ra, shift)                                                                                               \
    ((dpuinstruction_t)(0x8c0020000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSR_ADDzrrici(rb, ra, shift, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x8c0020000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRX_Srri(dc, ra, shift)                                                                                                 \
    ((dpuinstruction_t)(0x9083400d0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRX_Srric(dc, ra, shift, cc)                                                                                            \
    ((dpuinstruction_t)(0x9083400d0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRX_Srrici(dc, ra, shift, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0x9083400d0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRX_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9080d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSRX_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9080d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSRX_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9080d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRX_Urri(dc, ra, shift)                                                                                                 \
    ((dpuinstruction_t)(0x9003400d0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRX_Urric(dc, ra, shift, cc)                                                                                            \
    ((dpuinstruction_t)(0x9003400d0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRX_Urrici(dc, ra, shift, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0x9003400d0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRX_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9000d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSRX_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9000d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSRX_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9000d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRXrri(rc, ra, shift)                                                                                                   \
    ((dpuinstruction_t)(0x8003400d0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRXrric(rc, ra, shift, cc)                                                                                              \
    ((dpuinstruction_t)(0x8003400d0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRXrrici(rc, ra, shift, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x8003400d0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRXrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x8000d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSRXrrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8000d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSRXrrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8000d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRXzri(ra, shift)                                                                                                       \
    ((dpuinstruction_t)(0x8c03400d0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRXzric(ra, shift, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8c03400d0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define LSRXzrici(ra, shift, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8c03400d0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LSRXzrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x8c00d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define LSRXzrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c00d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define LSRXzrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c00d0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define LW_Serri(endian, dc, ra, off)                                                                                            \
    ((dpuinstruction_t)(0x708364000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LW_Srri(dc, ra, off)                                                                                                     \
    ((dpuinstruction_t)(0x708364000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LW_Uerri(endian, dc, ra, off)                                                                                            \
    ((dpuinstruction_t)(0x700364000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LW_Urri(dc, ra, off)                                                                                                     \
    ((dpuinstruction_t)(0x700364000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define LWerri(endian, rc, ra, off)                                                                                              \
    ((dpuinstruction_t)(0x700344000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LWersi(endian, rc, ra, off)                                                                                              \
    ((dpuinstruction_t)(0x700354000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LWrri(rc, ra, off)                                                                                                       \
    ((dpuinstruction_t)(0x700344000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define LWSrri(rc, ra, off)                                                                                                      \
    ((dpuinstruction_t)(0x700354000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define MOVDrr(dc, db)                                                                                                           \
    ((dpuinstruction_t)(0x806020010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)))
#define MOVDrrci(dc, db, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x806020010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MOVE_Sri(dc, imm)                                                                                                        \
    ((dpuinstruction_t)(0x50eb00000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define MOVE_Srici(dc, imm, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x90e3b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MOVE_Srr(dc, ra)                                                                                                         \
    ((dpuinstruction_t)(0x9083b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MOVE_Srrci(dc, ra, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x9083b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MOVE_Uri(dc, imm)                                                                                                        \
    ((dpuinstruction_t)(0x506b00000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define MOVE_Urici(dc, imm, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x9063b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MOVE_Urr(dc, ra)                                                                                                         \
    ((dpuinstruction_t)(0x9003b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MOVE_Urrci(dc, ra, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x9003b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MOVEri(rc, imm)                                                                                                          \
    ((dpuinstruction_t)(0x606300000000l | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define MOVErici(rc, imm, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8063b0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MOVErr(rc, ra)                                                                                                           \
    ((dpuinstruction_t)(0x8003b0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MOVErrci(rc, ra, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8003b0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_SH_Srrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c8000700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_SH_SH_Srrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c8000700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_SH_Srrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c8000700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_SHrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_SH_SHrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_SHrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_SHzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_SH_SHzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_SHzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000700000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_SL_Srrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c8000600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_SH_SL_Srrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c8000600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_SL_Srrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c8000600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_SLrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_SH_SLrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_SLrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_SLzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_SH_SLzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_SLzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000600000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_UH_Srrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c8000b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_SH_UH_Srrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c8000b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_UH_Srrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c8000b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_UHrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_SH_UHrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_UHrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_UHzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_SH_UHzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_UHzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_UL_Srrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c8000a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_SH_UL_Srrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c8000a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_UL_Srrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c8000a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_ULrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_SH_ULrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_ULrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SH_ULzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_SH_ULzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SH_ULzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_SH_Srrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c8000500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_SL_SH_Srrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c8000500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_SH_Srrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c8000500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_SHrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_SL_SHrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_SHrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_SHzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_SL_SHzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_SHzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000500000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_SL_Srrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c8000400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_SL_SL_Srrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c8000400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_SL_Srrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c8000400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_SLrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_SL_SLrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_SLrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_SLzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_SL_SLzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_SLzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000400000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_UH_Srrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c8000900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_SL_UH_Srrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c8000900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_UH_Srrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c8000900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_UHrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_SL_UHrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_UHrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_UHzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_SL_UHzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_UHzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_UL_Srrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c8000800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_SL_UL_Srrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c8000800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_UL_Srrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c8000800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_ULrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_SL_ULrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_ULrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_SL_ULzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_SL_ULzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_SL_ULzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_STEPrrri(dc, ra, db, shift)                                                                                          \
    ((dpuinstruction_t)(0x800040010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define MUL_STEPrrrici(dc, ra, db, shift, cc, pc)                                                                                \
    ((dpuinstruction_t)(0x800040010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UH_UH_Urrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c0000300000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_UH_UH_Urrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c0000300000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UH_UH_Urrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c0000300000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UH_UHrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000300000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_UH_UHrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000300000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UH_UHrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000300000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UH_UHzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000300000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_UH_UHzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000300000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UH_UHzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000300000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UH_UL_Urrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c0000200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_UH_UL_Urrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c0000200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UH_UL_Urrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c0000200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UH_ULrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_UH_ULrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UH_ULrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UH_ULzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_UH_ULzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UH_ULzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UL_UH_Urrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c0000100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_UL_UH_Urrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c0000100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UL_UH_Urrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c0000100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UL_UHrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_UL_UHrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UL_UHrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UL_UHzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_UL_UHzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UL_UHzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000100000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UL_UL_Urrr(dc, ra, rb)                                                                                               \
    ((dpuinstruction_t)(0x4c0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define MUL_UL_UL_Urrrc(dc, ra, rb, cc)                                                                                          \
    ((dpuinstruction_t)(0x4c0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UL_UL_Urrrci(dc, ra, rb, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x4c0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40) | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UL_ULrrr(rc, ra, rb)                                                                                                 \
    ((dpuinstruction_t)(0xc0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define MUL_UL_ULrrrc(rc, ra, rb, cc)                                                                                            \
    ((dpuinstruction_t)(0xc0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UL_ULrrrci(rc, ra, rb, cc, pc)                                                                                       \
    ((dpuinstruction_t)(0xc0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39) | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)                          \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define MUL_UL_ULzrr(ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x3c0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define MUL_UL_ULzrrc(ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x3c0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define MUL_UL_ULzrrci(ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x3c0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NAND_Srric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x9083f0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NAND_Srrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x9083f0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NAND_Srrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x9083f0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NAND_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9080c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NAND_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9080c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NAND_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9080c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NAND_Urric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x9003f0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NAND_Urrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x9003f0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NAND_Urrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x9003f0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NAND_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9000c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NAND_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9000c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NAND_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9000c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NANDrri(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0x8003f0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NANDrric(rc, ra, imm, cc)                                                                                                \
    ((dpuinstruction_t)(0x8003f0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NANDrrici(rc, ra, imm, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x8003f0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NANDrrif(rc, ra, imm)                                                                                                    \
    ((dpuinstruction_t)(0x8003f0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NANDrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x8000c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NANDrrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8000c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NANDrrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8000c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NANDzric(ra, imm, cc)                                                                                                    \
    ((dpuinstruction_t)(0x8c03f0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39) | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define NANDzrici(ra, imm, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x8c03f0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 44) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NANDzrif(ra, imm)                                                                                                        \
    ((dpuinstruction_t)(0x8c03f0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define NANDzrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x8c00c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NANDzrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c00c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NANDzrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c00c0f00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NEGrr(rc, ra)                                                                                                            \
    ((dpuinstruction_t)(0x200300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NEGrrci(rc, ra, cc, pc)                                                                                                  \
    ((dpuinstruction_t)(0xc0340000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NOP() ((dpuinstruction_t)(0x7c6300000000l))
#define NOR_Srric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x9083e0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NOR_Srrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9083e0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NOR_Srrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x9083e0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NOR_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9080c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NOR_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9080c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NOR_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9080c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NOR_Urric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x9003e0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NOR_Urrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9003e0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NOR_Urrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x9003e0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NOR_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9000c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NOR_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9000c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NOR_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9000c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NORrri(rc, ra, imm)                                                                                                      \
    ((dpuinstruction_t)(0x8003e0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NORrric(rc, ra, imm, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8003e0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NORrrici(rc, ra, imm, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8003e0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NORrrif(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0x8003e0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NORrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0x8000c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NORrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8000c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NORrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8000c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NORzric(ra, imm, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c03e0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39) | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define NORzrici(ra, imm, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c03e0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 44) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NORzrif(ra, imm)                                                                                                         \
    ((dpuinstruction_t)(0x8c03e0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define NORzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x8c00c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NORzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x8c00c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NORzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8c00c0e00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NOTrci(ra, cc, pc)                                                                                                       \
    ((dpuinstruction_t)(0x9f8380ff0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NOTrr(rc, ra)                                                                                                            \
    ((dpuinstruction_t)(0x4003ffffffffl | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NOTrrci(rc, ra, cc, pc)                                                                                                  \
    ((dpuinstruction_t)(0x800380ff0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NXOR_Srric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x908390000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NXOR_Srrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x908390000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NXOR_Srrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x908390000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NXOR_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9080c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NXOR_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9080c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NXOR_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9080c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NXOR_Urric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x900390000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NXOR_Urrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x900390000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NXOR_Urrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x900390000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NXOR_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x9000c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NXOR_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x9000c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NXOR_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9000c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NXORrri(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0x800390000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NXORrric(rc, ra, imm, cc)                                                                                                \
    ((dpuinstruction_t)(0x800390000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NXORrrici(rc, ra, imm, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x800390000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NXORrrif(rc, ra, imm)                                                                                                    \
    ((dpuinstruction_t)(0x800390000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define NXORrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x8000c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NXORrrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8000c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NXORrrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8000c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NXORzric(ra, imm, cc)                                                                                                    \
    ((dpuinstruction_t)(0x8c0390000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39) | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define NXORzrici(ra, imm, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x8c0390000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 44) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define NXORzrif(ra, imm)                                                                                                        \
    ((dpuinstruction_t)(0x8c0390000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define NXORzrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x8c00c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define NXORzrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c00c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define NXORzrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c00c0900000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define OR_Srri(dc, rb, imm)                                                                                                     \
    ((dpuinstruction_t)(0x40e000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34)                        \
        | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 44) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define OR_Srric(dc, ra, imm, cc)                                                                                                \
    ((dpuinstruction_t)(0x9083b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define OR_Srrici(dc, ra, imm, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9083b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define OR_Srrif(dc, ra, imm)                                                                                                    \
    ((dpuinstruction_t)(0x9083b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define OR_Srrr(dc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x9080c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define OR_Srrrc(dc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0x9080c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define OR_Srrrci(dc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x9080c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define OR_Urri(dc, rb, imm)                                                                                                     \
    ((dpuinstruction_t)(0x406000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34)                        \
        | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 44) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define OR_Urric(dc, ra, imm, cc)                                                                                                \
    ((dpuinstruction_t)(0x9003b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define OR_Urrici(dc, ra, imm, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9003b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define OR_Urrif(dc, ra, imm)                                                                                                    \
    ((dpuinstruction_t)(0x9003b0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define OR_Urrr(dc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0x9000c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define OR_Urrrc(dc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0x9000c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define OR_Urrrci(dc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x9000c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORrri(rc, ra, imm)                                                                                                       \
    ((dpuinstruction_t)(0x600300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)                    \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define ORrric(rc, ra, imm, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8003b0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ORrrici(rc, ra, imm, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8003b0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORrrif(rc, ra, imm)                                                                                                      \
    ((dpuinstruction_t)(0x8003b0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ORrrr(rc, ra, rb)                                                                                                        \
    ((dpuinstruction_t)(0x8000c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ORrrrc(rc, ra, rb, cc)                                                                                                   \
    ((dpuinstruction_t)(0x8000c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ORrrrci(rc, ra, rb, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x8000c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORzri(rb, imm)                                                                                                           \
    ((dpuinstruction_t)(0x666000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34) | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define ORzric(ra, imm, cc)                                                                                                      \
    ((dpuinstruction_t)(0x8c03b0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39) | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define ORzrici(ra, imm, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8c03b0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 44) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORzrif(ra, imm)                                                                                                          \
    ((dpuinstruction_t)(0x8c03b0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define ORzrr(ra, rb)                                                                                                            \
    ((dpuinstruction_t)(0x8c00c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ORzrrc(ra, rb, cc)                                                                                                       \
    ((dpuinstruction_t)(0x8c00c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ORzrrci(ra, rb, cc, pc)                                                                                                  \
    ((dpuinstruction_t)(0x8c00c0b00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORN_Srric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x9083d0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ORN_Srrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9083d0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORN_Srrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x9083d0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ORN_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9080c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ORN_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9080c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ORN_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9080c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORN_Urric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x9003d0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ORN_Urrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x9003d0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORN_Urrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x9003d0000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ORN_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9000c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ORN_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9000c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ORN_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9000c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORNrri(rc, ra, imm)                                                                                                      \
    ((dpuinstruction_t)(0x8003d0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ORNrric(rc, ra, imm, cc)                                                                                                 \
    ((dpuinstruction_t)(0x8003d0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ORNrrici(rc, ra, imm, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x8003d0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORNrrif(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0x8003d0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define ORNrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0x8000c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ORNrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8000c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ORNrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8000c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORNzric(ra, imm, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c03d0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39) | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define ORNzrici(ra, imm, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c03d0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 44) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ORNzrif(ra, imm)                                                                                                         \
    ((dpuinstruction_t)(0x8c03d0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define ORNzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x8c00c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ORNzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x8c00c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ORNzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8c00c0d00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define READ_RUNrici(ra, imm, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x7c0330000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RELEASErici(ra, imm, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x7c0380000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1f) << 26) | (((((dpuinstruction_t)imm) >> 13) & 0x7) << 39)                      \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0) | (((((dpuinstruction_t)cc) >> 0) & 0x3) << 24)))
#define RESUMEri(ra, imm)                                                                                                        \
    ((dpuinstruction_t)(0x7d0320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)))
#define RESUMErici(ra, imm, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x7d0320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROL_Srri(dc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x908340020000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROL_Srric(dc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x908340020000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROL_Srrici(dc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x908340020000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROL_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9080d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ROL_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9080d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ROL_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9080d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROL_Urri(dc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x900340020000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROL_Urric(dc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x900340020000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROL_Urrici(dc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x900340020000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROL_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9000d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ROL_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9000d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ROL_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9000d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROLrri(rc, ra, shift)                                                                                                    \
    ((dpuinstruction_t)(0x800340020000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROLrric(rc, ra, shift, cc)                                                                                               \
    ((dpuinstruction_t)(0x800340020000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROLrrici(rc, ra, shift, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x800340020000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROLrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0x8000d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ROLrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8000d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ROLrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8000d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROLzri(ra, shift)                                                                                                        \
    ((dpuinstruction_t)(0x8c0340020000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROLzric(ra, shift, cc)                                                                                                   \
    ((dpuinstruction_t)(0x8c0340020000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROLzrici(ra, shift, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x8c0340020000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROLzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x8c00d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ROLzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x8c00d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ROLzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8c00d0200000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROL_ADD_Srrri(dc, rb, ra, shift)                                                                                         \
    ((dpuinstruction_t)(0x908000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROL_ADD_Srrrici(dc, rb, ra, shift, cc, pc)                                                                               \
    ((dpuinstruction_t)(0x908000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROL_ADD_Urrri(dc, rb, ra, shift)                                                                                         \
    ((dpuinstruction_t)(0x900000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROL_ADD_Urrrici(dc, rb, ra, shift, cc, pc)                                                                               \
    ((dpuinstruction_t)(0x900000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROL_ADDrrri(rc, rb, ra, shift)                                                                                           \
    ((dpuinstruction_t)(0x800000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROL_ADDrrrici(rc, rb, ra, shift, cc, pc)                                                                                 \
    ((dpuinstruction_t)(0x800000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROL_ADDzrri(rb, ra, shift)                                                                                               \
    ((dpuinstruction_t)(0x8c0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROL_ADDzrrici(rb, ra, shift, cc, pc)                                                                                     \
    ((dpuinstruction_t)(0x8c0000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROR_Srri(dc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x9083400a0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROR_Srric(dc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x9083400a0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROR_Srrici(dc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x9083400a0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROR_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9080d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ROR_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9080d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ROR_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9080d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROR_Urri(dc, ra, shift)                                                                                                  \
    ((dpuinstruction_t)(0x9003400a0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROR_Urric(dc, ra, shift, cc)                                                                                             \
    ((dpuinstruction_t)(0x9003400a0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define ROR_Urrici(dc, ra, shift, cc, pc)                                                                                        \
    ((dpuinstruction_t)(0x9003400a0000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define ROR_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9000d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define ROR_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9000d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define ROR_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9000d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RORrri(rc, ra, shift)                                                                                                    \
    ((dpuinstruction_t)(0x8003400a0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                      \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define RORrric(rc, ra, shift, cc)                                                                                               \
    ((dpuinstruction_t)(0x8003400a0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define RORrrici(rc, ra, shift, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x8003400a0000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RORrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0x8000d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define RORrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8000d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define RORrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8000d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RORzri(ra, shift)                                                                                                        \
    ((dpuinstruction_t)(0x8c03400a0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define RORzric(ra, shift, cc)                                                                                                   \
    ((dpuinstruction_t)(0x8c03400a0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20)                       \
        | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)))
#define RORzrici(ra, shift, cc, pc)                                                                                              \
    ((dpuinstruction_t)(0x8c03400a0000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)shift) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)shift) >> 4) & 0x1) << 28)                    \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RORzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x8c00d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define RORzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x8c00d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define RORzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8c00d0a00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)cc) >> 4) & 0x1) << 29)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RSUB_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x4c8040c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define RSUB_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c8040c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define RSUB_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c8040c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RSUB_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x4c0040c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define RSUB_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c0040c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define RSUB_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c0040c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RSUBrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0xc0040c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define RSUBrrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0xc0040c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define RSUBrrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0xc0040c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RSUBzrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x3c0040c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define RSUBzrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0040c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define RSUBzrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x3c0040c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RSUBC_Srrr(dc, ra, rb)                                                                                                   \
    ((dpuinstruction_t)(0x4c8060c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define RSUBC_Srrrc(dc, ra, rb, cc)                                                                                              \
    ((dpuinstruction_t)(0x4c8060c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define RSUBC_Srrrci(dc, ra, rb, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x4c8060c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RSUBC_Urrr(dc, ra, rb)                                                                                                   \
    ((dpuinstruction_t)(0x4c0060c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define RSUBC_Urrrc(dc, ra, rb, cc)                                                                                              \
    ((dpuinstruction_t)(0x4c0060c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define RSUBC_Urrrci(dc, ra, rb, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x4c0060c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RSUBCrrr(rc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0xc0060c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define RSUBCrrrc(rc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0xc0060c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define RSUBCrrrci(rc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0xc0060c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define RSUBCzrr(ra, rb)                                                                                                         \
    ((dpuinstruction_t)(0x3c0060c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define RSUBCzrrc(ra, rb, cc)                                                                                                    \
    ((dpuinstruction_t)(0x3c0060c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24)))
#define RSUBCzrrci(ra, rb, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x3c0060c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SATS_Srr(dc, ra)                                                                                                         \
    ((dpuinstruction_t)(0x908320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define SATS_Srrc(dc, ra, cc)                                                                                                    \
    ((dpuinstruction_t)(0x908320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define SATS_Srrci(dc, ra, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x908320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SATS_Urr(dc, ra)                                                                                                         \
    ((dpuinstruction_t)(0x900320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)))
#define SATS_Urrc(dc, ra, cc)                                                                                                    \
    ((dpuinstruction_t)(0x900320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define SATS_Urrci(dc, ra, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x900320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SATSrr(rc, ra)                                                                                                           \
    ((dpuinstruction_t)(0x800320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)))
#define SATSrrc(rc, ra, cc)                                                                                                      \
    ((dpuinstruction_t)(0x800320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define SATSrrci(rc, ra, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x800320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SATSzr(ra) ((dpuinstruction_t)(0x8c0320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define SATSzrc(ra, cc)                                                                                                          \
    ((dpuinstruction_t)(0x8c0320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define SATSzrci(ra, cc, pc)                                                                                                     \
    ((dpuinstruction_t)(0x8c0320000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SBerii(endian, ra, off, imm)                                                                                             \
    ((dpuinstruction_t)(0x7c0340000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 0)))
#define SBerir(endian, ra, off, rb)                                                                                              \
    ((dpuinstruction_t)(0x7c0040000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SBesii(endian, ra, off, imm)                                                                                             \
    ((dpuinstruction_t)(0x7c0350000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 0)))
#define SBesir(endian, ra, off, rb)                                                                                              \
    ((dpuinstruction_t)(0x7c0050000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SBrii(ra, off, imm)                                                                                                      \
    ((dpuinstruction_t)(0x7c0340000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 0)))
#define SBrir(ra, off, rb)                                                                                                       \
    ((dpuinstruction_t)(0x7c0040000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SB_IDerii(endian, ra, off, imm)                                                                                          \
    ((dpuinstruction_t)(0x7c0360000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 0)))
#define SB_IDri(ra, off)                                                                                                         \
    ((dpuinstruction_t)(0x7c0360000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)))
#define SB_IDrii(ra, off, imm)                                                                                                   \
    ((dpuinstruction_t)(0x7c0360000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 0)))
#define SBSrii(ra, off, imm)                                                                                                     \
    ((dpuinstruction_t)(0x7c0350000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 0)))
#define SBSrir(ra, off, rb)                                                                                                      \
    ((dpuinstruction_t)(0x7c0050000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SDerii(endian, ra, off, imm)                                                                                             \
    ((dpuinstruction_t)(0x7c0346000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SDerir(endian, ra, off, db)                                                                                              \
    ((dpuinstruction_t)(0x7c0046010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SDesii(endian, ra, off, imm)                                                                                             \
    ((dpuinstruction_t)(0x7c0356000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SDesir(endian, ra, off, db)                                                                                              \
    ((dpuinstruction_t)(0x7c0056010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SDrii(ra, off, imm)                                                                                                      \
    ((dpuinstruction_t)(0x7c0346000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SDrir(ra, off, db)                                                                                                       \
    ((dpuinstruction_t)(0x7c0046010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SD_IDerii(endian, ra, off, imm)                                                                                          \
    ((dpuinstruction_t)(0x7c0366000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SD_IDri(ra, off)                                                                                                         \
    ((dpuinstruction_t)(0x7c0366000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)))
#define SD_IDrii(ra, off, imm)                                                                                                   \
    ((dpuinstruction_t)(0x7c0366000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SDMArri(ra, rb, immDma)                                                                                                  \
    ((dpuinstruction_t)(0x700000000002l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)immDma) >> 0) & 0xff) << 24)))
#define SDSrii(ra, off, imm)                                                                                                     \
    ((dpuinstruction_t)(0x7c0356000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SDSrir(ra, off, db)                                                                                                      \
    ((dpuinstruction_t)(0x7c0056010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SHerii(endian, ra, off, imm)                                                                                             \
    ((dpuinstruction_t)(0x7c0342000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SHerir(endian, ra, off, rb)                                                                                              \
    ((dpuinstruction_t)(0x7c0042000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SHesii(endian, ra, off, imm)                                                                                             \
    ((dpuinstruction_t)(0x7c0352000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SHesir(endian, ra, off, rb)                                                                                              \
    ((dpuinstruction_t)(0x7c0052000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SHrii(ra, off, imm)                                                                                                      \
    ((dpuinstruction_t)(0x7c0342000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SHrir(ra, off, rb)                                                                                                       \
    ((dpuinstruction_t)(0x7c0042000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SH_IDerii(endian, ra, off, imm)                                                                                          \
    ((dpuinstruction_t)(0x7c0362000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SH_IDri(ra, off)                                                                                                         \
    ((dpuinstruction_t)(0x7c0362000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)))
#define SH_IDrii(ra, off, imm)                                                                                                   \
    ((dpuinstruction_t)(0x7c0362000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SHSrii(ra, off, imm)                                                                                                     \
    ((dpuinstruction_t)(0x7c0352000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SHSrir(ra, off, rb)                                                                                                      \
    ((dpuinstruction_t)(0x7c0052000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define STOP() ((dpuinstruction_t)(0x7ef320000000l))
#define STOPci(cc, pc)                                                                                                           \
    ((dpuinstruction_t)(0x7ef320000000l | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUB_Srirc(dc, imm, ra, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c8340000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUB_Srirci(dc, imm, ra, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c8340000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUB_Srirf(dc, imm, ra)                                                                                                   \
    ((dpuinstruction_t)(0x4c8340000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUB_Srric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c8380000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUB_Srrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c8380000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUB_Srrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x4c8380000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUB_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x4c8080c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define SUB_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x4c8080c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)))
#define SUB_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x4c8080c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUB_Urirc(dc, imm, ra, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c0340000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUB_Urirci(dc, imm, ra, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c0340000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUB_Urirf(dc, imm, ra)                                                                                                   \
    ((dpuinstruction_t)(0x4c0340000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUB_Urric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c0380000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUB_Urrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c0380000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUB_Urrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x4c0380000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUB_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x4c0080c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define SUB_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x4c0080c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)))
#define SUB_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x4c0080c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBrir(rc, imm, ra)                                                                                                      \
    ((dpuinstruction_t)(0x200300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define SUBrirc(rc, imm, ra, cc)                                                                                                 \
    ((dpuinstruction_t)(0xc0340000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBrirci(rc, imm, ra, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0xc0340000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBrirf(rc, imm, ra)                                                                                                     \
    ((dpuinstruction_t)(0xc0340000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBrric(rc, ra, imm, cc)                                                                                                 \
    ((dpuinstruction_t)(0xc0380000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBrrici(rc, ra, imm, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0xc0380000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBrrif(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0xc0380000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0xc0080c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define SUBrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0xc0080c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)))
#define SUBrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0xc0080c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBssi(rc, ra, imm)                                                                                                      \
    ((dpuinstruction_t)(0xc0390000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0x1f) << 0) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 5)                       \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 6) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 7)                        \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 8) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 9)                        \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 10) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 11)))
#define SUBsss(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0xc0050c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x7) << 17) | (((((dpuinstruction_t)ra) >> 3) & 0x3) << 32)))
#define SUBzir(imm, rb)                                                                                                          \
    ((dpuinstruction_t)(0x626000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34) | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define SUBzirc(imm, ra, cc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0340000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define SUBzirci(imm, ra, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x3c0340000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBzirf(imm, ra)                                                                                                         \
    ((dpuinstruction_t)(0x3c0340000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define SUBzric(ra, imm, cc)                                                                                                     \
    ((dpuinstruction_t)(0x3c0380000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define SUBzrici(ra, imm, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x3c0380000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBzrif(ra, imm)                                                                                                         \
    ((dpuinstruction_t)(0x3c0380000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define SUBzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x3c0080c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define SUBzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x3c0080c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)))
#define SUBzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x3c0080c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBC_Srirc(dc, imm, ra, cc)                                                                                              \
    ((dpuinstruction_t)(0x4c8360000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBC_Srirci(dc, imm, ra, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x4c8360000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBC_Srirf(dc, imm, ra)                                                                                                  \
    ((dpuinstruction_t)(0x4c8360000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBC_Srric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x4c83a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBC_Srrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x4c83a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBC_Srrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x4c83a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBC_Srrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x4c80a0c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define SUBC_Srrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c80a0c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)))
#define SUBC_Srrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c80a0c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBC_Urirc(dc, imm, ra, cc)                                                                                              \
    ((dpuinstruction_t)(0x4c0360000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBC_Urirci(dc, imm, ra, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x4c0360000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBC_Urirf(dc, imm, ra)                                                                                                  \
    ((dpuinstruction_t)(0x4c0360000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBC_Urric(dc, ra, imm, cc)                                                                                              \
    ((dpuinstruction_t)(0x4c03a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBC_Urrici(dc, ra, imm, cc, pc)                                                                                         \
    ((dpuinstruction_t)(0x4c03a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBC_Urrif(dc, ra, imm)                                                                                                  \
    ((dpuinstruction_t)(0x4c03a0000000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBC_Urrr(dc, ra, rb)                                                                                                    \
    ((dpuinstruction_t)(0x4c00a0c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define SUBC_Urrrc(dc, ra, rb, cc)                                                                                               \
    ((dpuinstruction_t)(0x4c00a0c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)))
#define SUBC_Urrrci(dc, ra, rb, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x4c00a0c00000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBCrir(rc, imm, ra)                                                                                                     \
    ((dpuinstruction_t)(0x300300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define SUBCrirc(rc, imm, ra, cc)                                                                                                \
    ((dpuinstruction_t)(0xc0360000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBCrirci(rc, imm, ra, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0xc0360000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBCrirf(rc, imm, ra)                                                                                                    \
    ((dpuinstruction_t)(0xc0360000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBCrric(rc, ra, imm, cc)                                                                                                \
    ((dpuinstruction_t)(0xc03a0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBCrrici(rc, ra, imm, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0xc03a0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBCrrif(rc, ra, imm)                                                                                                    \
    ((dpuinstruction_t)(0xc03a0000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define SUBCrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0xc00a0c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define SUBCrrrc(rc, ra, rb, cc)                                                                                                 \
    ((dpuinstruction_t)(0xc00a0c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)))
#define SUBCrrrci(rc, ra, rb, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0xc00a0c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBCzir(imm, rb)                                                                                                         \
    ((dpuinstruction_t)(0x636000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34) | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define SUBCzirc(imm, ra, cc)                                                                                                    \
    ((dpuinstruction_t)(0x3c0360000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define SUBCzirci(imm, ra, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x3c0360000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBCzirf(imm, ra)                                                                                                        \
    ((dpuinstruction_t)(0x3c0360000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define SUBCzric(ra, imm, cc)                                                                                                    \
    ((dpuinstruction_t)(0x3c03a0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define SUBCzrici(ra, imm, cc, pc)                                                                                               \
    ((dpuinstruction_t)(0x3c03a0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBCzrif(ra, imm)                                                                                                        \
    ((dpuinstruction_t)(0x3c03a0000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)))
#define SUBCzrr(ra, rb)                                                                                                          \
    ((dpuinstruction_t)(0x3c00a0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define SUBCzrrc(ra, rb, cc)                                                                                                     \
    ((dpuinstruction_t)(0x3c00a0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)))
#define SUBCzrrci(ra, rb, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x3c00a0c00000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0x1f) << 24) | (((((dpuinstruction_t)cc) >> 5) & 0x1) << 30)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SUBSrri(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0xc0390000000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0x1f) << 0) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 5)                       \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 6) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 7)                        \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 8) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 9)                        \
        | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 10) | (((((dpuinstruction_t)imm) >> 16) & 0x1) << 11)))
#define SUBSrrr(rc, ra, rb)                                                                                                      \
    ((dpuinstruction_t)(0xc0050c00000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)rb) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x7) << 17) | (((((dpuinstruction_t)ra) >> 3) & 0x3) << 32)))
#define SWerii(endian, ra, off, imm)                                                                                             \
    ((dpuinstruction_t)(0x7c0344000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SWerir(endian, ra, off, rb)                                                                                              \
    ((dpuinstruction_t)(0x7c0044000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SWesii(endian, ra, off, imm)                                                                                             \
    ((dpuinstruction_t)(0x7c0354000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SWesir(endian, ra, off, rb)                                                                                              \
    ((dpuinstruction_t)(0x7c0054000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SWrii(ra, off, imm)                                                                                                      \
    ((dpuinstruction_t)(0x7c0344000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SWrir(ra, off, rb)                                                                                                       \
    ((dpuinstruction_t)(0x7c0044000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define SW_IDerii(endian, ra, off, imm)                                                                                          \
    ((dpuinstruction_t)(0x7c0364000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)endian) >> 0) & 0x1) << 27) | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20)                     \
        | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39) | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SW_IDri(ra, off)                                                                                                         \
    ((dpuinstruction_t)(0x7c0364000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)))
#define SW_IDrii(ra, off, imm)                                                                                                   \
    ((dpuinstruction_t)(0x7c0364000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SWAPDrr(dc, db)                                                                                                          \
    ((dpuinstruction_t)(0x8060a0010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)))
#define SWAPDrrci(dc, db, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8060a0010000l | (((((dpuinstruction_t)db) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)db) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define SWSrii(ra, off, imm)                                                                                                     \
    ((dpuinstruction_t)(0x7c0354000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 16)                       \
        | (((((dpuinstruction_t)imm) >> 4) & 0xfff) << 0)))
#define SWSrir(ra, off, rb)                                                                                                      \
    ((dpuinstruction_t)(0x7c0054000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)off) >> 7) & 0x1) << 24) | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define TELLri(ra, off)                                                                                                          \
    ((dpuinstruction_t)(0x7c0300000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)off) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)off) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)off) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)off) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)off) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)off) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)off) >> 12) & 0xfff) << 0)))
#define TIME_Sr(dc)                                                                                                              \
    ((dpuinstruction_t)(0x4ce020800000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define TIME_Srci(dc, cc, pc)                                                                                                    \
    ((dpuinstruction_t)(0x4ce020800000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define TIME_Ur(dc)                                                                                                              \
    ((dpuinstruction_t)(0x4c6020800000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define TIME_Urci(dc, cc, pc)                                                                                                    \
    ((dpuinstruction_t)(0x4c6020800000l | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define TIMEr(rc)                                                                                                                \
    ((dpuinstruction_t)(0xc6020800000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define TIMErci(rc, cc, pc)                                                                                                      \
    ((dpuinstruction_t)(0xc6020800000l | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                                           \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define TIMEz() ((dpuinstruction_t)(0x3c6020800000l))
#define TIMEzci(cc, pc)                                                                                                          \
    ((dpuinstruction_t)(0x3c6020800000l | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define TIME_CFG_Srr(dc, rb)                                                                                                     \
    ((dpuinstruction_t)(0x4ce420800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define TIME_CFG_Srrci(dc, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x4ce420800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define TIME_CFG_Urr(dc, rb)                                                                                                     \
    ((dpuinstruction_t)(0x4c6420800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44)))
#define TIME_CFG_Urrci(dc, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x4c6420800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0x3) << 40)                          \
        | (((((dpuinstruction_t)dc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define TIME_CFGr(rb)                                                                                                            \
    ((dpuinstruction_t)(0x3c6420800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32)))
#define TIME_CFGrr(rc, rb)                                                                                                       \
    ((dpuinstruction_t)(0xc6420800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                          \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44)))
#define TIME_CFGrrci(rc, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0xc6420800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                           \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x7) << 39)                          \
        | (((((dpuinstruction_t)rc) >> 3) & 0x3) << 44) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define TIME_CFGzr(rb)                                                                                                           \
    ((dpuinstruction_t)(0x3c6420800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32)))
#define TIME_CFGzrci(rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x3c6420800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                          \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define XOR_Srric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x908380000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define XOR_Srrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x908380000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define XOR_Srrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x908380000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define XOR_Srrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9080c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define XOR_Srrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9080c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define XOR_Srrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9080c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define XOR_Urric(dc, ra, imm, cc)                                                                                               \
    ((dpuinstruction_t)(0x900380000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define XOR_Urrici(dc, ra, imm, cc, pc)                                                                                          \
    ((dpuinstruction_t)(0x900380000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define XOR_Urrif(dc, ra, imm)                                                                                                   \
    ((dpuinstruction_t)(0x900380000000l | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define XOR_Urrr(dc, ra, rb)                                                                                                     \
    ((dpuinstruction_t)(0x9000c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define XOR_Urrrc(dc, ra, rb, cc)                                                                                                \
    ((dpuinstruction_t)(0x9000c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define XOR_Urrrci(dc, ra, rb, cc, pc)                                                                                           \
    ((dpuinstruction_t)(0x9000c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)dc) >> 1) & 0xf) << 40)                          \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define XORrri(rc, ra, imm)                                                                                                      \
    ((dpuinstruction_t)(0x400300000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define XORrric(rc, ra, imm, cc)                                                                                                 \
    ((dpuinstruction_t)(0x800380000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define XORrrici(rc, ra, imm, cc, pc)                                                                                            \
    ((dpuinstruction_t)(0x800380000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define XORrrif(rc, ra, imm)                                                                                                     \
    ((dpuinstruction_t)(0x800380000000l | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                        \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)))
#define XORrrr(rc, ra, rb)                                                                                                       \
    ((dpuinstruction_t)(0x8000c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define XORrrrc(rc, ra, rb, cc)                                                                                                  \
    ((dpuinstruction_t)(0x8000c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define XORrrrci(rc, ra, rb, cc, pc)                                                                                             \
    ((dpuinstruction_t)(0x8000c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)rc) >> 0) & 0x1f) << 39)                         \
        | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34) | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)                         \
        | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define XORzri(rb, imm)                                                                                                          \
    ((dpuinstruction_t)(0x646000000000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0x7) << 34) | (((((dpuinstruction_t)imm) >> 7) & 0x1) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0xff) << 24)))
#define XORzric(ra, imm, cc)                                                                                                     \
    ((dpuinstruction_t)(0x8c0380000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15)                        \
        | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14) | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13)                       \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12) | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0)                     \
        | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39) | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define XORzrici(ra, imm, cc, pc)                                                                                                \
    ((dpuinstruction_t)(0x8c0380000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20)                         \
        | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16) | (((((dpuinstruction_t)imm) >> 8) & 0x7) << 39)                        \
        | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 44) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))
#define XORzrif(ra, imm)                                                                                                         \
    ((dpuinstruction_t)(0x8c0380000000l | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                                         \
        | (((((dpuinstruction_t)imm) >> 0) & 0xf) << 20) | (((((dpuinstruction_t)imm) >> 4) & 0xf) << 16)                        \
        | (((((dpuinstruction_t)imm) >> 8) & 0x1) << 15) | (((((dpuinstruction_t)imm) >> 9) & 0x1) << 14)                        \
        | (((((dpuinstruction_t)imm) >> 10) & 0x1) << 13) | (((((dpuinstruction_t)imm) >> 11) & 0x1) << 12)                      \
        | (((((dpuinstruction_t)imm) >> 12) & 0xfff) << 0) | (((((dpuinstruction_t)imm) >> 24) & 0x7) << 39)                     \
        | (((((dpuinstruction_t)imm) >> 27) & 0x1) << 44)))
#define XORzrr(ra, rb)                                                                                                           \
    ((dpuinstruction_t)(0x8c00c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)))
#define XORzrrc(ra, rb, cc)                                                                                                      \
    ((dpuinstruction_t)(0x8c00c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24)))
#define XORzrrci(ra, rb, cc, pc)                                                                                                 \
    ((dpuinstruction_t)(0x8c00c0800000l | (((((dpuinstruction_t)rb) >> 0) & 0x7) << 17)                                          \
        | (((((dpuinstruction_t)rb) >> 3) & 0x3) << 32) | (((((dpuinstruction_t)ra) >> 0) & 0x1f) << 34)                         \
        | (((((dpuinstruction_t)cc) >> 0) & 0xf) << 24) | (((((dpuinstruction_t)pc) >> 0) & 0xffff) << 0)))

#endif // __DPU_INSTRUCTION_ENCODER_H
