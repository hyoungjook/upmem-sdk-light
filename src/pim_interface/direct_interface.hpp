#pragma once

#include <immintrin.h>
#include <x86intrin.h>

#include <cinttypes>
#include <iostream>
#include <cassert>
#include <cstdio>
#include <string>
#include <vector>

extern "C" {
#include <dpu.h>
#include <dpu_internals.h>
#include <dpu_management.h>
#include <dpu_program.h>
#include <dpu_loader.h>
#include <dpu_rank.h>
#include <dpu_region_address_translation.h>
#include <dpu_target_macros.h>
#include <dpu_types.h>
#include <ufi_ci_commands.h>
#include <ufi_config.h>
#include <ufi_runner.h>

#include "backends/hw/src/commons/dpu_region_constants.h"
#include "backends/hw/src/rank/hw_dpu_sysfs.h"
#include "backends/ufi/include/ufi/ufi.h"

typedef struct _fpga_allocation_parameters_t {
    bool activate_ila;
    bool activate_filtering_ila;
    bool activate_mram_bypass;
    bool activate_mram_refresh_emulation;
    unsigned int mram_refresh_emulation_period;
    char *report_path;
    bool cycle_accurate;
} fpga_allocation_parameters_t;

typedef struct _hw_dpu_rank_allocation_parameters_t {
    struct dpu_rank_fs rank_fs;
    struct dpu_region_address_translation translate;
    uint64_t region_size;
    uint8_t mode, dpu_chip_id, backend_id;
    uint8_t channel_id;
    uint8_t *ptr_region;
    bool bypass_module_compatibility;
    /* Backends specific */
    fpga_allocation_parameters_t fpga;
} *hw_dpu_rank_allocation_parameters_t;

typedef struct _dpu_properties_property {
    const char *name;
    const char *value;
    bool used;
} * _dpu_properties_property_t;

typedef struct _dpu_props {
    unsigned int nr_properties;
    _dpu_properties_property_t properties;
} * _dpu_props_t;

typedef struct _hw_dpu_rank_context_t {
    /* Hybrid mode: Address of control interfaces when memory mapped
     * Perf mode:   Base region address, mappings deal with offset to target control interfaces
     * Safe mode:   Buffer handed to the driver
     */
    uint64_t *control_interfaces;
} * hw_dpu_rank_context_t;
}

#ifdef __AVX512F__
#define USE_AVX512
#endif

const uint32_t MAX_NR_RANKS = 40;
const uint32_t DPU_PER_RANK = 64;
const uint64_t MRAM_SIZE = (64 << 20);

class DirectPIMInterface {
   public:
    DirectPIMInterface(uint32_t nr_of_ranks, bool debuggable = true) {
        this->debuggable = debuggable;
        AllocRanks(nr_of_ranks);
        load_from_dpu_set();
        if (nr_of_ranks != DPU_ALLOCATE_ALL) {
            assert(this->nr_of_ranks == nr_of_ranks);
        }
    }

    void load_from_dpu_set() {
        DPU_ASSERT(dpu_get_nr_dpus(dpu_set, &this->nr_of_dpus));
        DPU_ASSERT(dpu_get_nr_ranks(dpu_set, &this->nr_of_ranks));
        std::printf("Allocated %d DPU(s)\n", this->nr_of_dpus);
        std::printf("Allocated %d Ranks(s)\n", this->nr_of_ranks);
        assert(this->nr_of_dpus <= nr_of_ranks * DPU_PER_RANK);

        ranks = new dpu_rank_t *[nr_of_ranks];
        params = new hw_dpu_rank_allocation_parameters_t[nr_of_ranks];
        base_addrs = new uint8_t *[nr_of_ranks];
        for (uint32_t i = 0; i < nr_of_ranks; i++) {
            ranks[i] = dpu_set.list.ranks[i];
            params[i] =
                ((hw_dpu_rank_allocation_parameters_t)(ranks[i]
                                                           ->description
                                                           ->_internals.data));
            base_addrs[i] = params[i]->ptr_region;
        }
    }

    ~DirectPIMInterface() {
        //if (nr_of_ranks > 0) {
        //    DPU_ASSERT(dpu_free(dpu_set));
        //    nr_of_ranks = nr_of_dpus = 0;
        //}
    }

    inline bool aligned(uint64_t offset, uint64_t factor) {
        return (offset % factor == 0);
    }

    inline uint64_t GetCorrectOffsetMRAM(uint64_t address_offset,
                                         uint32_t dpu_id) {
        auto FastPath = [](uint64_t address_offset, uint32_t dpu_id) {
            uint64_t mask_move_7 =
                (~((1 << 22) - 1)) + (1 << 13);              // 31..22, 13
            uint64_t mask_move_6 = ((1 << 22) - (1 << 15));  // 21..15
            uint64_t mask_move_14 = (1 << 14);               // 14
            uint64_t mask_move_4 = (1 << 13) - 1;            // 12 .. 0
            return ((address_offset & mask_move_7) << 7) |
                   ((address_offset & mask_move_6) << 6) |
                   ((address_offset & mask_move_14) << 14) |
                   ((address_offset & mask_move_4) << 4) | (dpu_id << 18);
        };

        // not used
        auto OraclePath = [](uint64_t address_offset, uint32_t dpu_id) {
            // uint64_t fastoffset = get_correct_offset_fast(address_offset,
            // dpu_id);
            uint64_t offset = 0;
            // 1 : address_offset < 64MB
            offset += (512ll << 20) * (address_offset >> 22);
            address_offset &= (1ll << 22) - 1;
            // 2 : address_offset < 4MB
            if (address_offset & (16 << 10)) {
                offset += (256ll << 20);
            }
            offset += (2ll << 20) * (address_offset / (32 << 10));
            address_offset %= (16 << 10);
            // 3 : address_offset < 16K
            if (address_offset & (8 << 10)) {
                offset += (1ll << 20);
            }
            address_offset %= (8 << 10);
            offset += address_offset * 16;
            // 4 : address_offset < 8K
            offset += (dpu_id & 3) * (256 << 10);
            // 5
            if (dpu_id >= 4) {
                offset += 64;
            }
            return offset;
        };
        (void)OraclePath;

        // uint64_t v1 = FastPath(address_offset, dpu_id);
        // uint64_t v2 = OraclePath(address_offset, dpu_id);
        // assert(v1 == v2);
        // return v1;

        return FastPath(address_offset, dpu_id);
    }

    void ReceiveFromRankMRAM(uint8_t **buffers, uint32_t symbol_offset,
                             uint8_t *ptr_dest, uint32_t length) {
        assert(aligned(symbol_offset, sizeof(uint64_t)));
        assert(aligned(length, sizeof(uint64_t)));
        assert((uint64_t)symbol_offset + length <= MRAM_SIZE);

        uint64_t cache_line[8], cache_line_interleave[8];

        for (uint32_t dpu_id = 0; dpu_id < 4; ++dpu_id) {
            for (uint32_t i = 0; i < length / sizeof(uint64_t); ++i) {
                // 8 shards of DPUs
                uint64_t offset =
                    GetCorrectOffsetMRAM(symbol_offset + (i * 8), dpu_id);
                __builtin_ia32_clflushopt((void *)(ptr_dest + offset));
                offset += 0x40;
                __builtin_ia32_clflushopt((void *)(ptr_dest + offset));
            }
        }
        __builtin_ia32_mfence();

        auto LoadData = [](uint64_t *cache_line, uint8_t *ptr_dest) {
            cache_line[0] = *((volatile uint64_t *)((uint8_t *)ptr_dest +
                                                    0 * sizeof(uint64_t)));
            cache_line[1] = *((volatile uint64_t *)((uint8_t *)ptr_dest +
                                                    1 * sizeof(uint64_t)));
            cache_line[2] = *((volatile uint64_t *)((uint8_t *)ptr_dest +
                                                    2 * sizeof(uint64_t)));
            cache_line[3] = *((volatile uint64_t *)((uint8_t *)ptr_dest +
                                                    3 * sizeof(uint64_t)));
            cache_line[4] = *((volatile uint64_t *)((uint8_t *)ptr_dest +
                                                    4 * sizeof(uint64_t)));
            cache_line[5] = *((volatile uint64_t *)((uint8_t *)ptr_dest +
                                                    5 * sizeof(uint64_t)));
            cache_line[6] = *((volatile uint64_t *)((uint8_t *)ptr_dest +
                                                    6 * sizeof(uint64_t)));
            cache_line[7] = *((volatile uint64_t *)((uint8_t *)ptr_dest +
                                                    7 * sizeof(uint64_t)));
        };

        for (uint32_t dpu_id = 0; dpu_id < 4; ++dpu_id) {
            for (uint32_t i = 0; i < length / sizeof(uint64_t); ++i) {
                if ((i % 8 == 0) && (i + 8 < length / sizeof(uint64_t))) {
                    for (int j = 0; j < 16; j++) {
                        __builtin_prefetch(
                            ((uint64_t *)buffers[j * 4 + dpu_id]) + i + 8);
                    }
                }
                uint64_t offset =
                    GetCorrectOffsetMRAM(symbol_offset + (i * 8), dpu_id);
                __builtin_prefetch(ptr_dest + offset + 0x40 * 6);
                __builtin_prefetch(ptr_dest + offset + 0x40 * 7);

                LoadData(cache_line, ptr_dest + offset);
                byte_interleave_avx2(cache_line, cache_line_interleave);
                for (int j = 0; j < 8; j++) {
                    if (buffers[j * 8 + dpu_id] == nullptr) {
                        continue;
                    }
                    *(((uint64_t *)buffers[j * 8 + dpu_id]) + i) =
                        cache_line_interleave[j];
                }

                offset += 0x40;
                LoadData(cache_line, ptr_dest + offset);
                byte_interleave_avx2(cache_line, cache_line_interleave);
                for (int j = 0; j < 8; j++) {
                    if (buffers[j * 8 + dpu_id + 4] == nullptr) {
                        continue;
                    }
                    *(((uint64_t *)buffers[j * 8 + dpu_id + 4]) + i) =
                        cache_line_interleave[j];
                }
            }
        }

        __builtin_ia32_mfence();
    }

    void SendToRankMRAM(uint8_t **buffers, uint32_t symbol_offset,
                        uint8_t *ptr_dest, uint32_t length) {
        assert(aligned(symbol_offset, sizeof(uint64_t)));
        assert(aligned(length, sizeof(uint64_t)));
        assert((uint64_t)symbol_offset + length <= MRAM_SIZE);

        uint64_t cache_line[8];

        for (uint32_t dpu_id = 0; dpu_id < 4; ++dpu_id) {
            for (uint32_t i = 0; i < length / sizeof(uint64_t); ++i) {
                if ((i % 8 == 0) && (i + 8 < length / sizeof(uint64_t))) {
                    for (int j = 0; j < 16; j++) {
                        __builtin_prefetch(
                            ((uint64_t *)buffers[j * 4 + dpu_id]) + i + 8);
                    }
                }
                uint64_t offset =
                    GetCorrectOffsetMRAM(symbol_offset + (i * 8), dpu_id);

                for (int j = 0; j < 8; j++) {
                    if (buffers[j * 8 + dpu_id] == nullptr) {
                        continue;
                    }
                    cache_line[j] =
                        *(((uint64_t *)buffers[j * 8 + dpu_id]) + i);
                }
                // avx512 is faster (due to stream writes?)
                #ifdef USE_AVX512
                byte_interleave_avx512(cache_line, (uint64_t *)(ptr_dest + offset), true);
                #else
                byte_interleave_avx2(cache_line, (uint64_t *)(ptr_dest + offset));
                __builtin_ia32_clflushopt((void *)(ptr_dest + offset));
                #endif

                offset += 0x40;
                for (int j = 0; j < 8; j++) {
                    if (buffers[j * 8 + dpu_id + 4] == nullptr) {
                        continue;
                    }
                    cache_line[j] =
                        *(((uint64_t *)buffers[j * 8 + dpu_id + 4]) + i);
                }
                #ifdef USE_AVX512
                byte_interleave_avx512(cache_line, (uint64_t *)(ptr_dest + offset), true);
                #else
                byte_interleave_avx2(cache_line, (uint64_t *)(ptr_dest + offset));
                __builtin_ia32_clflushopt((void *)(ptr_dest + offset));
                #endif
            }
        }
        __builtin_ia32_mfence();
    }

    void BroadcastToRankMRAM(uint8_t *buffer, uint32_t symbol_offset,
                             uint8_t *ptr_dest, uint32_t length) {
        assert(aligned(symbol_offset, sizeof(uint64_t)));
        assert(aligned(length, sizeof(uint64_t)));
        assert((uint64_t)symbol_offset + length <= MRAM_SIZE);

        uint64_t cache_value;

        for (uint32_t i = 0; i < length / sizeof(uint64_t); ++i) {
            if ((i % 8 == 0) && (i + 8 < length / sizeof(uint64_t))) {
                __builtin_prefetch(
                    ((uint64_t *)buffer) + i + 8);
            }

            cache_value = *(((uint64_t *)buffer) + i);

            for (uint32_t dpu_id = 0; dpu_id < 4; ++dpu_id) {
                uint64_t offset =
                    GetCorrectOffsetMRAM(symbol_offset + (i * 8), dpu_id);
                
                #ifdef USE_AVX512
                byte_interleave_avx512_bc(cache_value, (uint64_t*)(ptr_dest + offset), true);
                #else
                byte_interleave_avx2_bc(cache_value, (uint64_t*)(ptr_dest + offset));
                __builtin_ia32_clflushopt((void *)(ptr_dest + offset));
                #endif

                offset += 0x40;
                #ifdef USE_AVX512
                byte_interleave_avx512_bc(cache_value, (uint64_t*)(ptr_dest + offset), true);
                #else
                byte_interleave_avx2_bc(cache_value, (uint64_t*)(ptr_dest + offset));
                __builtin_ia32_clflushopt((void *)(ptr_dest + offset));
                #endif
            }
        }

        __builtin_ia32_mfence();
    }

    bool DirectAvailable() {
        for (uint32_t i = 0; i < nr_of_ranks; i++) {
            if (params[i]->mode != DPU_REGION_MODE_PERF) {
                return false;
            }
        }

        return true;
    }

    // Find symbol address offset
    uint32_t GetSymbolOffset(std::string symbol_name) {
        dpu_symbol_t symbol;
        DPU_ASSERT(dpu_get_symbol(&program, symbol_name.c_str(), &symbol));
        return symbol.address;
    }

   public:
    void AllocRanks(uint32_t nr_ranks) {
        if (debuggable) {
            DPU_ASSERT(dpu_alloc_ranks(nr_ranks, "nrThreadsPerRank=1", &dpu_set));
            return;
        }

        dpu_rank_t **dpu_ranks;
        dpu_ranks = (dpu_rank_t**)calloc(nr_ranks, sizeof(dpu_rank_t*));
        for (uint32_t each_rank = 0; each_rank < nr_ranks; each_rank++) {
            // dpu_get_rank_of_type
            struct _dpu_props properties;
            properties.nr_properties = 0;
            properties.properties = NULL;
            dpu_rank_t *dpu_rank = (dpu_rank_t*)calloc(1, sizeof(dpu_rank_t));
            dpu_rank->type = HW;
            assert(dpu_rank_handler_instantiate(dpu_rank->type, &dpu_rank->handler_context, true));            
            assert(dpu_rank_handler_get_rank(dpu_rank, dpu_rank->handler_context, &properties));
            dpu_rank->profiling_context.dpu = DPU_GET_UNSAFE(dpu_rank, 0, 0);
            dpu_rank->profiling_context.enable_profiling = DPU_PROFILING_NOP;
            dpu_rank->description->configuration.disable_reset_on_alloc = false;
            dpu_rank->debug.cmds_buffer.size = 1000;
            dpu_rank->debug.cmds_buffer.cmds = NULL;

            // dpu_reset_rank
            dpu_rank->handler_context->handler->custom_operation(
                dpu_rank, (dpu_slice_id_t)-1, (dpu_member_id_t)-1,
                DPU_COMMAND_EVENT_START, (dpu_custom_command_args_t)DPU_EVENT_RESET);
            dpu_rank->handler_context->handler->custom_operation(
                dpu_rank, (dpu_slice_id_t)-1, (dpu_member_id_t)-1,
                DPU_COMMAND_ALL_SOFT_RESET, NULL);
            ci_reset_rank(dpu_rank);
            dpu_rank->handler_context->handler->custom_operation(
                dpu_rank, (dpu_slice_id_t)-1, (dpu_member_id_t)-1,
                DPU_COMMAND_EVENT_END, (dpu_custom_command_args_t)DPU_EVENT_RESET);
            dpu_ranks[each_rank] = dpu_rank;
        }

        // init_dpu_set
        memset(&dpu_set, 0, sizeof(dpu_set_t));
        dpu_set.kind = DPU_SET_RANKS;
        dpu_set.list.nr_ranks = nr_ranks;
        dpu_set.list.ranks = dpu_ranks;
    }

    void Load(const char *dpu_binary) {
        if (debuggable) {
            DPU_ASSERT(dpu_load(dpu_set, dpu_binary, NULL));
            // find program pointer
            DPU_FOREACH(dpu_set, dpu, each_dpu) {
                assert(dpu.kind == DPU_SET_DPU);
                dpu_t *dpuptr = dpu.dpu;
                if (!dpu_is_enabled(dpuptr)) {
                    continue;
                }
                dpu_program_t *this_program = dpu_get_program(dpuptr);
                memcpy(&program, this_program, sizeof(dpu_program_t));
                break;
            }
            return;
        }

        dpu_elf_file_t elf_info;
        dpu_init_program_ref(&program);
        DPU_ASSERT(dpu_load_elf_program(&elf_info, dpu_binary, &program,
            ranks[0]->description->hw.memories.mram_size));
        for (uint32_t i = 0; i < nr_of_ranks; i++) {
            struct _dpu_loader_context_t loader_context;
            dpu_loader_fill_rank_context(&loader_context, ranks[i]);
            DPU_ASSERT(dpu_elf_load(elf_info, &loader_context));
            for (int j = 0; j < MAX_NR_DPUS_PER_RANK; j++) {
                if (ranks[i]->dpus[j].enabled) {
                    dpu_take_program_ref(&program);
                    dpu_set_program(&ranks[i]->dpus[j], &program);
                }
            }
        }
    }

    void SwitchMux(int rank_id, bool set_mux_for_host) {
        switch_mux_for(rank_id, set_mux_for_host);
    }

    bool SwitchMuxRequired(int rank_id, bool set_mux_for_host) {
        return switch_mux_for_required(rank_id, set_mux_for_host);
    }

    void SwitchMuxForStart(int rank_id, bool set_mux_for_host) {
        switch_mux_for_start(rank_id, set_mux_for_host);
    }

    void SwitchMuxForSync(int rank_id, bool set_mux_for_host) {
        switch_mux_for_sync(rank_id, set_mux_for_host);
    }
    
    void Launch(uint32_t rank_id) {
        // use default launch if you need the default "error handling"
        if (debuggable) {
            if (rank_id == 0) {
                DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));
            }
            return;
        }

        LaunchAsync(rank_id);
        LaunchAsyncWait(rank_id);
    }

    void LaunchAsync(uint32_t rank_id) {
        assert(!debuggable);
        dpu_rank_t *rank = ranks[rank_id];
        //DPU_ASSERT(ci_start_thread_rank(rank, DPU_BOOT_THREAD, false, NULL));
        //return;

        //switch_mux_for(rank_id, false);

        uint8_t ci_mask = ALL_CIS;
        DPU_ASSERT((dpu_error_t)ufi_select_all(rank, &ci_mask));
        DPU_ASSERT((dpu_error_t)ufi_thread_boot(rank, ci_mask, DPU_BOOT_THREAD, NULL));
    }

    void LaunchAsyncWait(uint32_t rank_id) {
        assert(!debuggable);
        dpu_rank_t *rank = ranks[rank_id];
        dpu_slice_id_t ci_cnt = rank->description->hw.topology.nr_of_control_interfaces;
        dpu_bitfield_t dpu_poll_running[DPU_MAX_NR_CIS];
        dpu_bitfield_t dpu_poll_in_fault[DPU_MAX_NR_CIS];
        while (true) {
            DPU_ASSERT(ci_poll_rank(rank, dpu_poll_running, dpu_poll_in_fault));
            dpu_slice_id_t done_cnt = 0;
            for (dpu_slice_id_t each_slice = 0; each_slice < ci_cnt; ++each_slice) {
                dpu_selected_mask_t mask_all = rank->runtime.control_interface.slice_info[each_slice].enabled_dpus;
                assert((dpu_poll_in_fault[each_slice] & mask_all) == 0);
                if ((dpu_poll_running[each_slice] & mask_all) == 0) {
                    done_cnt++;
                }
            }
            if (done_cnt == ci_cnt) {
                break;
            }
            //usleep(10);
        }
    }

    void PrintLog() {
        assert(debuggable);
        dpu_set_t dpu;
        DPU_FOREACH(dpu_set, dpu) {
            DPU_ASSERT(dpu_log_read(dpu, stdout));
        }
    }

    struct NormalBufferInfo {
        uint8_t *buffers[MAX_NR_RANKS * MAX_NR_DPUS_PER_RANK];
        uint32_t symbol_offset;
    };
    struct BroadcastBufferInfo {
        uint8_t *buffer;
        uint32_t symbol_offset;
    };

    size_t RegisterNormalBuffer(uint8_t **buffers, std::string symbol_name, uint32_t symbol_offset) {
        // Please make sure buffers don't overflow
        assert(DirectAvailable());

        NormalBufferInfo *info = new NormalBufferInfo;
        uint32_t symbol_base_offset = GetSymbolOffset(symbol_name);
        assert(symbol_base_offset & MRAM_ADDRESS_SPACE);
        info->symbol_offset = (symbol_base_offset ^ MRAM_ADDRESS_SPACE) + symbol_offset;
        // Skip disabled PIM modules
        {
            uint32_t offset = 0;
            for (uint32_t i = 0; i < nr_of_ranks; i++) {
                for (int j = 0; j < MAX_NR_DPUS_PER_RANK; j++) {
                    if (ranks[i]->dpus[j].enabled) {
                        info->buffers[i * MAX_NR_DPUS_PER_RANK + j] =
                            buffers[offset++];
                    } else {
                        info->buffers[i * MAX_NR_DPUS_PER_RANK + j] =
                            nullptr;
                    }
                }
            }
            assert(offset == nr_of_dpus);
        }

        size_t info_handle = normal_buffer_infos.size();
        normal_buffer_infos.push_back(info);
        return info_handle;
    }

    void ReceiveFromPIMRank(uint32_t rank_id, size_t info_handle, uint32_t length) {
        //DPU_ASSERT(dpu_switch_mux_for_rank(ranks[rank_id], true));
        //switch_mux_for(rank_id, true);
        NormalBufferInfo *info = normal_buffer_infos[info_handle];
        ReceiveFromRankMRAM(&info->buffers[rank_id * MAX_NR_DPUS_PER_RANK],
                            info->symbol_offset, base_addrs[rank_id], length);
    }

    void SendToPIMRank(uint32_t rank_id, size_t info_handle, uint32_t length) {
        //DPU_ASSERT(dpu_switch_mux_for_rank(ranks[rank_id], true));
        //switch_mux_for(rank_id, true);
        NormalBufferInfo *info = normal_buffer_infos[info_handle];
        SendToRankMRAM(&info->buffers[rank_id * MAX_NR_DPUS_PER_RANK],
                       info->symbol_offset, base_addrs[rank_id], length);
    }

    size_t RegisterBroadcastBuffer(uint8_t *buffer, std::string symbol_name, uint32_t symbol_offset) {
        // Please make sure buffers don't overflow
        assert(DirectAvailable());
        BroadcastBufferInfo info;
        info.buffer = buffer;
        uint32_t symbol_base_offset = GetSymbolOffset(symbol_name);
        assert(symbol_base_offset & MRAM_ADDRESS_SPACE);
        info.symbol_offset = (symbol_base_offset ^ MRAM_ADDRESS_SPACE) + symbol_offset;

        size_t info_handle = broadcast_buffer_infos.size();
        broadcast_buffer_infos.push_back(info);
        return info_handle;
    }

    void BroadcastToPIMRank(uint32_t rank_id, size_t info_handle, uint32_t length) {
        //DPU_ASSERT(dpu_switch_mux_for_rank(ranks[rank_id], true));
        //switch_mux_for(rank_id, true);
        BroadcastBufferInfo &info = broadcast_buffer_infos[info_handle];
        BroadcastToRankMRAM(info.buffer,
                            info.symbol_offset, base_addrs[rank_id], length);
    }

   private:
    void byte_interleave_avx2(uint64_t *input, uint64_t *output) {
        __m256i tm = _mm256_set_epi8(
            15, 11, 7, 3, 14, 10, 6, 2, 13, 9, 5, 1, 12, 8, 4, 0,

            15, 11, 7, 3, 14, 10, 6, 2, 13, 9, 5, 1, 12, 8, 4, 0);
        char *src1 = (char *)input, *dst1 = (char *)output;

        __m256i vindex = _mm256_setr_epi32(0, 8, 16, 24, 32, 40, 48, 56);
        __m256i perm = _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7);

        __m256i load0 = _mm256_i32gather_epi32((int *)src1, vindex, 1);
        __m256i load1 = _mm256_i32gather_epi32((int *)(src1 + 4), vindex, 1);

        __m256i transpose0 = _mm256_shuffle_epi8(load0, tm);
        __m256i transpose1 = _mm256_shuffle_epi8(load1, tm);

        __m256i final0 = _mm256_permutevar8x32_epi32(transpose0, perm);
        __m256i final1 = _mm256_permutevar8x32_epi32(transpose1, perm);

        _mm256_storeu_si256((__m256i *)&dst1[0], final0);
        _mm256_storeu_si256((__m256i *)&dst1[32], final1);
    }

    void byte_interleave_avx2_bc(uint64_t input_val, uint64_t *output) {
        __m256i tm = _mm256_set_epi8(
            15, 11, 7, 3, 14, 10, 6, 2, 13, 9, 5, 1, 12, 8, 4, 0,

            15, 11, 7, 3, 14, 10, 6, 2, 13, 9, 5, 1, 12, 8, 4, 0);
        char *src1 = (char *)&input_val, *dst1 = (char *)output;

        __m256i perm = _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7);

        __m256i load0 = _mm256_set1_epi32(*(int*)src1);
        __m256i load1 = _mm256_set1_epi32(*(int*)(src1 + 4));

        __m256i transpose0 = _mm256_shuffle_epi8(load0, tm);
        __m256i transpose1 = _mm256_shuffle_epi8(load1, tm);

        __m256i final0 = _mm256_permutevar8x32_epi32(transpose0, perm);
        __m256i final1 = _mm256_permutevar8x32_epi32(transpose1, perm);

        _mm256_storeu_si256((__m256i *)&dst1[0], final0);
        _mm256_storeu_si256((__m256i *)&dst1[32], final1);
    }

    #ifdef USE_AVX512
    void byte_interleave_avx512(uint64_t *input, uint64_t *output,
                                bool use_stream) {
        __m512i mask;

        mask = _mm512_set_epi64(0x0f0b07030e0a0602ULL, 0x0d0905010c080400ULL,

                                0x0f0b07030e0a0602ULL, 0x0d0905010c080400ULL,

                                0x0f0b07030e0a0602ULL, 0x0d0905010c080400ULL,

                                0x0f0b07030e0a0602ULL, 0x0d0905010c080400ULL);

        __m512i vindex = _mm512_setr_epi32(0, 8, 16, 24, 32, 40, 48, 56, 4, 12,
                                           20, 28, 36, 44, 52, 60);
        __m512i perm = _mm512_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7, 8, 12, 9, 13,
                                         10, 14, 11, 15);

        __m512i load = _mm512_i32gather_epi32(vindex, input, 1);
        __m512i transpose = _mm512_shuffle_epi8(load, mask);
        __m512i final = _mm512_permutexvar_epi32(perm, transpose);

        if (use_stream) {
            _mm512_stream_si512((__m512i *)output, final);
            return;
        }

        _mm512_storeu_si512((__m512i *)output, final);
    }

    void byte_interleave_avx512_bc(uint64_t input_val, uint64_t *output,
                                   bool use_stream) {
        __m512i mask;

        mask = _mm512_set_epi64(0x0f0b07030e0a0602ULL, 0x0d0905010c080400ULL,

                                0x0f0b07030e0a0602ULL, 0x0d0905010c080400ULL,

                                0x0f0b07030e0a0602ULL, 0x0d0905010c080400ULL,

                                0x0f0b07030e0a0602ULL, 0x0d0905010c080400ULL);

        __m512i vindex = _mm512_setr_epi32(0, 0, 0, 0, 0, 0, 0, 0, 4, 4,
                                           4, 4, 4, 4, 4, 4);
        __m512i perm = _mm512_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7, 8, 12, 9, 13,
                                         10, 14, 11, 15);

        __m512i load = _mm512_i32gather_epi32(vindex, &input_val, 1);
        __m512i transpose = _mm512_shuffle_epi8(load, mask);
        __m512i final = _mm512_permutexvar_epi32(perm, transpose);

        if (use_stream) {
            _mm512_stream_si512((__m512i *)output, final);
            return;
        }

        _mm512_storeu_si512((__m512i *)output, final);
    }
    #endif

    void switch_mux_for(int rank_id, bool set_mux_for_host) {
        //DPU_ASSERT(dpu_switch_mux_for_rank(ranks[rank_id], set_mux_for_host));
        //return;
        if (!switch_mux_for_required(rank_id, set_mux_for_host)) return;
        switch_mux_for_start(rank_id, set_mux_for_host);
        switch_mux_for_sync(rank_id, set_mux_for_host);
    }

    bool switch_mux_for_required(int rank_id, bool set_mux_for_host) {
        dpu_rank_t *rank = ranks[rank_id];
        uint8_t nr_cis = rank->description->hw.topology.nr_of_control_interfaces;
        uint8_t nr_dpus_per_ci = rank->description->hw.topology.nr_of_dpus_per_control_interface;
        bool switch_mux = false;
        for (uint8_t each_slice = 0; each_slice < nr_cis; each_slice++) {
            if ((set_mux_for_host &&
		         __builtin_popcount(rank->runtime.control_interface
		    				.slice_info[each_slice]
		    				.host_mux_mram_state) <
		    	     nr_dpus_per_ci) ||
		        (!set_mux_for_host &&
		         rank->runtime.control_interface.slice_info[each_slice]
		    	     .host_mux_mram_state)) {
		    	switch_mux = true;
		    	break;
		    }
        }
        return switch_mux;
    }

    void switch_mux_for_start(int rank_id, bool set_mux_for_host) {
        dpu_rank_t *rank = ranks[rank_id];
        uint8_t nr_cis = rank->description->hw.topology.nr_of_control_interfaces;
        uint8_t nr_dpus_per_ci = rank->description->hw.topology.nr_of_dpus_per_control_interface;
        for (uint8_t each_slice = 0; each_slice < nr_cis; each_slice++) {
            rank->runtime.control_interface.slice_info[each_slice]
                .host_mux_mram_state =
                set_mux_for_host ? (1 << nr_dpus_per_ci) - 1 : 0x0;
        }

        uint8_t ci_mask = ALL_CIS;
        DPU_ASSERT((dpu_error_t)ufi_select_all_even_disabled(rank, &ci_mask));
	    DPU_ASSERT((dpu_error_t)ufi_set_mram_mux(rank, ci_mask, set_mux_for_host ? 0xFF : 0x0));
    }

    void switch_mux_for_sync(int rank_id, bool set_mux_for_host) {
        dpu_rank_t *rank = ranks[rank_id];
        uint8_t nr_cis = rank->description->hw.topology.nr_of_control_interfaces;
        uint8_t nr_dpus_per_ci = rank->description->hw.topology.nr_of_dpus_per_control_interface;
        uint8_t ci_mask = ALL_CIS;
        // dpu_check_wavegen_mux_status_for_rank
        DPU_ASSERT((dpu_error_t)ufi_write_dma_ctrl(rank, ci_mask, 0xFF, 0x02));
        DPU_ASSERT((dpu_error_t)ufi_clear_dma_ctrl(rank, ci_mask));
        uint8_t result_array[DPU_MAX_NR_CIS];
        uint8_t wavegen_expected = set_mux_for_host ? 0x00 : ((1 << 0) | (1 << 1));
        for (uint8_t each_dpu = 0; each_dpu < nr_dpus_per_ci; each_dpu++) {
            uint32_t timeout = 100;
            bool should_retry = false;
            uint8_t mask = ci_mask;
            do {
                DPU_ASSERT((dpu_error_t)ufi_read_dma_ctrl(rank, mask, result_array));

                for (uint8_t each_slice = 0; each_slice < nr_cis; each_slice++) {
                    if (!CI_MASK_ON(mask, each_slice)) continue;
                    if ((result_array[each_slice] & 0x7B) != wavegen_expected) {
                        should_retry = true;
                        break;
                    }
                }

                timeout--;
                //usleep(1);
            } while (timeout && should_retry);
        }
    }

   public:
    uint32_t GetNrOfRanks() const { return nr_of_ranks; }
    uint32_t GetNrOfDPUs() const { return nr_of_dpus; }

   protected:
    dpu_set_t dpu_set, dpu;
    uint32_t each_dpu;
    uint32_t nr_of_ranks, nr_of_dpus;
    bool debuggable;

    const int MRAM_ADDRESS_SPACE = 0x8000000;
    dpu_rank_t **ranks;
    hw_dpu_rank_allocation_parameters_t *params;
    uint8_t **base_addrs;
    dpu_program_t program;
    // map<std::string, uint32_t> offset_list;

    std::vector<NormalBufferInfo*> normal_buffer_infos;
    std::vector<BroadcastBufferInfo> broadcast_buffer_infos;

    uint8_t *send_buffers_aligned[MAX_NR_RANKS * MAX_NR_DPUS_PER_RANK];
    uint32_t send_buffers_symbol_offset, send_buffers_length;
    uint8_t *recv_buffers_aligned[MAX_NR_RANKS * MAX_NR_DPUS_PER_RANK];
    uint32_t recv_buffers_symbol_offset, recv_buffers_length;
    uint8_t *broadcast_buffer;
    uint32_t broadcast_buffer_symbol_offset, broadcast_buffer_length;
};
