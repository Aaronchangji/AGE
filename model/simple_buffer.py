import mmap
import posix_ipc
import random
import struct


class SimpleBuffer:
    # Size(8) | Content
    def __init__(self, fn_prefix, buffer_size):
        # Semaphores
        self.m2s_sem_name = fn_prefix + "_model_to_sys"
        self.m2s_sem = self._create_sem(self.m2s_sem_name, 0)
        self.s2m_sem_name = fn_prefix + "_sys_to_model"
        self.s2m_sem = self._create_sem(self.s2m_sem_name, 0)

        self.model_ready_name = fn_prefix + "_model_ready"
        self.model_ready_sem = self._create_sem(self.model_ready_name, 0)
        self.sys_ready_name = fn_prefix + "_sys_ready"
        self.sys_ready_sem = self._create_sem(self.sys_ready_name, 0)

        print(
            f"Values of Semaphores: m2s: {self.m2s_sem.value}, s2m: {self.s2m_sem.value}, model_ready: {self.model_ready_sem.value}, sys_ready: {self.sys_ready_sem.value}"
        )

        # Shared memory
        self.mem_name = fn_prefix + "_simple_buffer"
        self.buffer_size = buffer_size
        self.shared_memory = posix_ipc.SharedMemory(name=self.mem_name, flags=posix_ipc.O_CREAT, size=self.buffer_size)
        print(f"Shared memory size: {self.shared_memory.size}")
        self.buffer = mmap.mmap(self.shared_memory.fd, self.shared_memory.size)

    @staticmethod
    def _create_sem(sem_name, init_value):
        sem = posix_ipc.Semaphore(name=sem_name, flags=posix_ipc.O_CREAT, initial_value=init_value)
        sem.unlink()
        sem = posix_ipc.Semaphore(name=sem_name, flags=posix_ipc.O_CREAT, initial_value=init_value)
        return sem

    def reset_all_sem(self):
        self.m2s_sem.unlink()
        self.m2s_sem = posix_ipc.Semaphore(name=self.m2s_sem_name, flags=posix_ipc.O_CREAT, initial_value=0)
        self.s2m_sem.unlink()
        self.s2m_sem = posix_ipc.Semaphore(name=self.s2m_sem_name, flags=posix_ipc.O_CREAT, initial_value=0)
        self.model_ready_sem.unlink()
        self.model_ready_sem = posix_ipc.Semaphore(name=self.model_ready_name, flags=posix_ipc.O_CREAT, initial_value=0)
        self.sys_ready_sem.unlink()
        self.sys_ready_sem = posix_ipc.Semaphore(name=self.sys_ready_name, flags=posix_ipc.O_CREAT, initial_value=0)

    def read(self):
        # print(f"Try to acquire s2m Semaphore: {self.s2m_sem.value}")
        self.s2m_sem.acquire()
        # print(f"Acquired s2m Semaphore: {self.s2m_sem.value}")
        self.buffer.seek(0)
        data_size = struct.unpack("Q", self.buffer.read(8))[0]
        # print(f"Data size: {data_size}")
        self.buffer.seek(8)
        return self.buffer.read(data_size)

    def write(self, data, size):
        self.buffer.seek(0)
        self.buffer.write(size.to_bytes(8, byteorder="little"))
        self.buffer.seek(8)
        self.buffer.write(data)

        # print("Write to buffer and release m2s Semaphore")
        self.m2s_sem.release()

    def notify(self):
        self.m2s_sem.release()

    def wait_for_system(self):
        self.sys_ready_sem.acquire()

    def model_ready(self):
        self.model_ready_sem.release()
