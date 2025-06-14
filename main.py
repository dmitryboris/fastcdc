import hashlib
import sys
import time
from fastcdc import fastcdc


def hash_factory(data: bytes):
    return hashlib.sha256(data)


def get_size(obj):
    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        for key, value in obj.items():
            size += sys.getsizeof(key)
            size += get_size(value)  # Рекурсивно оцениваем размер значений
    elif isinstance(obj, list):
        for item in obj:
            size += get_size(item)  # Рекурсивно оцениваем размер элементов
    return size


def analyze_file(file_path):
    start_time = time.time()
    chunks = list(fastcdc(file_path, avg_size=16, hf=hash_factory))
    end_time1 = time.time()
    diff = end_time1 - start_time
    print(f"Время разбиения: {diff:.2f} секунд")

    # Подсчет дубликатов
    hash_map: dict = {}
    total_size = 0

    for chunk in chunks:
        h = chunk.hash
        if h in hash_map:
            hash_map[h][2] += 1
        else:
            hash_map[h] = [chunk.offset, chunk.length, 1]
        total_size += chunk.length
    gb_map_size = get_size(hash_map) / 1024 / 1024
    print(f'Размер мапы {gb_map_size} Мбайт')

    repeated_bytes = sum(
        length * (count - 1)
        for offset, length, count in hash_map.values()
        if count > 1
    )

    percent = (repeated_bytes / total_size) * 100 if total_size > 0 else 0

    end_time = time.time()
    elapsed = end_time - start_time

    print(
        f"\nПовторяющихся данных: {repeated_bytes / 1024 / 1024} Мбайт из {total_size / 1024 / 1024 / 1024} Гб (~{percent:.2f}%)")
    print(f"Время выполнения: {elapsed:.2f} секунд")


if __name__ == '__main__':
    file_path = ''
    analyze_file(file_path)
