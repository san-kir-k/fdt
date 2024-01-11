#pragma once


#define FOR_2_LOAD_LANES(func, from, to, stepsize, type) \
    func((type*)(from + stepsize), \
        func((type*)(from), to, 0), \
    1)

#define FOR_4_LOAD_LANES(func, from, to, stepsize, type) \
    func((type*)(from + 3 * stepsize), \
        func((type*)(from + 2 * stepsize), \
            func((type*)(from + stepsize), \
                func((type*)(from), to, 0), \
            1), \
        2), \
    3)

#define FOR_8_LOAD_LANES(func, from, to, stepsize, type) \
    func((type*)(from + 7 * stepsize), \
        func((type*)(from + 6 * stepsize), \
            func((type*)(from + 5 * stepsize), \
                func((type*)(from + 4 * stepsize), \
                    func((type*)(from + 3 * stepsize), \
                        func((type*)(from + 2 * stepsize), \
                            func((type*)(from + stepsize), \
                                func((type*)(from), to, 0), \
                            1), \
                        2), \
                    3), \
                4), \
            5), \
        6), \
    7)

#define FOR_16_LOAD_LANES(func, from, to, stepsize, type) \
    func((type*)(from + 15 * stepsize), \
        func((type*)(from + 14 * stepsize), \
            func((type*)(from + 13 * stepsize), \
                func((type*)(from + 12 * stepsize), \
                    func((type*)(from + 11 * stepsize), \
                        func((type*)(from + 10 * stepsize), \
                            func((type*)(from + 9 * stepsize), \
                                func((type*)(from + 8 * stepsize), \
                                    func((type*)(from + 7 * stepsize), \
                                        func((type*)(from + 6 * stepsize), \
                                            func((type*)(from + 5 * stepsize), \
                                                func((type*)(from + 4 * stepsize), \
                                                    func((type*)(from + 3 * stepsize), \
                                                        func((type*)(from + 2 * stepsize), \
                                                            func((type*)(from + stepsize), \
                                                                func((type*)(from), to, 0), \
                                                            1), \
                                                        2), \
                                                    3), \
                                                4), \
                                            5), \
                                        6), \
                                    7), \
                                8), \
                            9), \
                        10), \
                    11), \
                12), \
            13), \
        14), \
    15)
