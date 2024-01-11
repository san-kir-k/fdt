#pragma once


#define FOR_2_STORE_LANES(func, to, from, stepsize, type)       \
    do                                                          \
    {                                                           \
        func(reinterpret_cast<type*>(to), from, 0);             \
        func(reinterpret_cast<type*>(to + stepsize), from, 1);  \
    } while(0)                                                  \

#define FOR_4_STORE_LANES(func, to, from, stepsize, type)           \
    do                                                              \
    {                                                               \
        func(reinterpret_cast<type*>(to), from, 0);                 \
        func(reinterpret_cast<type*>(to + stepsize), from, 1);      \
        func(reinterpret_cast<type*>(to + 2 * stepsize), from, 2);  \
        func(reinterpret_cast<type*>(to + 3 * stepsize), from, 3);  \
    } while(0)                                                      \

#define FOR_8_STORE_LANES(func, to, from, stepsize, type)           \
    do                                                              \
    {                                                               \
        func(reinterpret_cast<type*>(to), from, 0);                 \
        func(reinterpret_cast<type*>(to + stepsize), from, 1);      \
        func(reinterpret_cast<type*>(to + 2 * stepsize), from, 2);  \
        func(reinterpret_cast<type*>(to + 3 * stepsize), from, 3);  \
        func(reinterpret_cast<type*>(to + 4 * stepsize), from, 4);  \
        func(reinterpret_cast<type*>(to + 5 * stepsize), from, 5);  \
        func(reinterpret_cast<type*>(to + 6 * stepsize), from, 6);  \
        func(reinterpret_cast<type*>(to + 7 * stepsize), from, 7);  \
    } while(0)                                                      \

#define FOR_16_STORE_LANES(func, to, from, stepsize, type)          \
    do                                                              \
    {                                                               \
        func(reinterpret_cast<type*>(to), from, 0);                 \
        func(reinterpret_cast<type*>(to + stepsize), from, 1);      \
        func(reinterpret_cast<type*>(to + 2 * stepsize), from, 2);  \
        func(reinterpret_cast<type*>(to + 3 * stepsize), from, 3);  \
        func(reinterpret_cast<type*>(to + 4 * stepsize), from, 4);  \
        func(reinterpret_cast<type*>(to + 5 * stepsize), from, 5);  \
        func(reinterpret_cast<type*>(to + 6 * stepsize), from, 6);  \
        func(reinterpret_cast<type*>(to + 7 * stepsize), from, 7);  \
        func(reinterpret_cast<type*>(to + 8 * stepsize), from, 8);  \
        func(reinterpret_cast<type*>(to + 9 * stepsize), from, 9);  \
        func(reinterpret_cast<type*>(to + 10 * stepsize), from, 10); \
        func(reinterpret_cast<type*>(to + 11 * stepsize), from, 11); \
        func(reinterpret_cast<type*>(to + 12 * stepsize), from, 12); \
        func(reinterpret_cast<type*>(to + 13 * stepsize), from, 13); \
        func(reinterpret_cast<type*>(to + 14 * stepsize), from, 14); \
        func(reinterpret_cast<type*>(to + 15 * stepsize), from, 15); \
    } while(0)                                                       \
