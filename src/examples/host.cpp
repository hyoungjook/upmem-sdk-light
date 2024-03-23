#include <stdio.h>
#include <stdint.h>
#include <unistd.h>

#include "direct_interface.hpp"
using namespace std;

int main() {

    DirectPIMInterface pimInterface(1, false);
    pimInterface.Load("dpu");

    int nr_of_dpus = pimInterface.GetNrOfDPUs();
    const int BUFFER_SIZE = 1024;
    uint8_t broadcast_buf[8];
    uint64_t checksum_base = 781;
    *(uint64_t*)broadcast_buf = checksum_base;
    uint8_t **input_buf = new uint8_t*[nr_of_dpus];
    uint64_t checksum[nr_of_dpus];
    for (int i = 0; i < nr_of_dpus; i++) {
        input_buf[i] = new uint8_t[BUFFER_SIZE];
        for (int j = 0; j < BUFFER_SIZE; j++) {
            input_buf[i][j] = (uint8_t)(i + j);
        }

        checksum[i] = checksum_base;
        uint64_t *array = (uint64_t*)input_buf[i];
        for (int j = 0; j < BUFFER_SIZE / 8; j++) {
            checksum[i] += array[j];
        }
    }

    const int OUT_BUFFER_SIZE = 8;
    uint8_t **output_buf = new uint8_t*[nr_of_dpus];
    for (int i = 0; i < nr_of_dpus; i++) {
        output_buf[i] = new uint8_t[OUT_BUFFER_SIZE];
    }

    size_t BROADCAST_HANDLE = pimInterface.RegisterBroadcastBuffer(broadcast_buf, "broadcast_buf", 0);
    size_t SEND_HANDLE = pimInterface.RegisterNormalBuffer(input_buf, "input_buf", 0);
    size_t RECV_HANDLE = pimInterface.RegisterNormalBuffer(output_buf, "output_buf", 0);

    // CPU -> PIM.MRAM broadcast
    pimInterface.BroadcastToPIMRank(0, BROADCAST_HANDLE, 8);

    // CPU -> PIM.MRAM : Supported by both direct and UPMEM interface.
    pimInterface.SendToPIMRank(0, SEND_HANDLE, BUFFER_SIZE);

    // Execute
    pimInterface.Launch(0);

    // PIM.MRAM -> CPU : Supported by both direct and UPMEM interface.
    pimInterface.ReceiveFromPIMRank(0, RECV_HANDLE, 8);

    //pimInterface.PrintLog();

    // Check
    for (int i = 0; i < nr_of_dpus; i++) {
        uint64_t output_checksum = *(uint64_t*)output_buf[i];
        if(checksum[i] != output_checksum) {
            printf("DPU[%d] failed: %lu != %lu\n", i, checksum[i], output_checksum);
            exit(1);
        }
        if (i % 10 == 0) {
            printf("DPU[%d] passed: %lu == %lu\n", i, checksum[i], output_checksum);
        }
    }

    for (int i = 0; i < nr_of_dpus; i++) {
        delete [] input_buf[i];
        delete [] output_buf[i];
    }
    delete [] input_buf;
    delete [] output_buf;

    return 0;
}