#include <iostream>
#include <cstdint>
#include <cstring>
#include <cassert>

#include "utility.h"
#include "aos.h"
#include "soa.h"

// -------------------------------------------------------------------------

// описание структуры в данном представлении
AoS_BEGIN(Point_AOS)
    AoS_ATTRIBUTE(x)
    AoS_ATTRIBUTE(y)
    AoS_ATTRIBUTE(z)
AoS_END(Point_AOS)

// шаблонная метаинформация о типах каждого поля структуры
using PointAttrs_AOS =  Attributes<
                            Point_AOS::x<uint64_t>,
                            Point_AOS::y<uint64_t>,
                            Point_AOS::z<uint64_t>
                        >;

// -------------------------------------------------------------------------

SoA_BEGIN(Point_SOA)
    SoA_ATTRIBUTE(x)
    SoA_ATTRIBUTE(y)
    SoA_ATTRIBUTE(z)
SoA_END(Point_SOA)

using PointAttrs_SOA =  Attributes<
                            Point_SOA::x<uint64_t>,
                            Point_SOA::y<uint64_t>,
                            Point_SOA::z<uint64_t>
                        >;

// -------------------------------------------------------------------------

// функция для проверки того, что при конвертации значения не испортились
bool ValidateEq(AoS<PointAttrs_AOS>& aos, SoA<PointAttrs_SOA>& soa)
{
    for (uint64_t i = 0; i < aos.Size(); ++i)
    {
        if (!(aos[i].x == soa.x[i] && aos[i].y == soa.y[i] && aos[i].z == soa.z[i]))
        {
            return false;
        }
    }
    return true;
}

void Naive(AoS<PointAttrs_AOS>& aos, SoA<PointAttrs_SOA>& soa)
{
    // установка значения в конкретное поле для всех элементов
    aos.SetValues<Point_AOS::x<uint64_t>>(1);
    aos.SetValues<Point_AOS::y<uint64_t>>(2);
    aos.SetValues<Point_AOS::z<uint64_t>>(3);

    // замер времени в микросекундах
    BENCHMARK("Naive AoS to SoA transformation: ", aos.ConvertToSoA, soa);
    assert(ValidateEq(aos, soa));

    soa.SetValues<Point_SOA::x<uint64_t>>(4);
    soa.SetValues<Point_SOA::y<uint64_t>>(5);
    soa.SetValues<Point_SOA::z<uint64_t>>(6);

    BENCHMARK("Naive SoA to AoS transformation: ", soa.ConvertToAoS, aos);
    assert(ValidateEq(aos, soa));
}

// -------------------------------------------------------------------------

int main()
{
    constexpr uint64_t size = 10'000'000;

    // инициализация двух представлений
    AoS<PointAttrs_AOS> aos{size};
    SoA<PointAttrs_SOA> soa{size};

    // бенчмарк наивной конвертации через memcpy
    Naive(aos, soa);

    return 0;
}
