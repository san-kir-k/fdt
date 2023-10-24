#pragma once

#include <cstdint>

#include "forward.h"

// ------------------------------------------------------------------------------------------

#define AoS_BEGIN(NAME)     \
    namespace NAME          \
    {

#define AoS_END(NAME)       \
    }

#define AoS_ATTRIBUTE(ATTR_NAME)                    \
    template <typename T>                           \
    class ATTR_NAME ## _Attribute                   \
    {                                               \
    public:                                         \
        T ATTR_NAME;                                \
                                                    \
    public:                                         \
        inline constexpr T& GetAttribute()          \
        {                                           \
            return ATTR_NAME;                       \
        }                                           \
                                                    \
    public:                                         \
        using Type = T;                             \
    };                                              \
                                                    \
    template <typename T>                           \
    using ATTR_NAME = ATTR_NAME ## _Attribute<T>;

// ------------------------------------------------------------------------------------------

template <typename... Attrs>
class AoS<Attributes<Attrs...>>
{
public:
    struct Struct: public Attrs...
    {
    };

public:
    AoS(uint64_t size)
        : m_size(size)
        , m_data(new Struct[m_size]{})
    {
    }

    AoS(const AoS&)             = delete;
    AoS& operator=(const AoS&)  = delete;
    AoS(AoS&&)                  = delete;
    AoS& operator=(AoS&&)       = delete;

    ~AoS()
    {
        delete[] m_data;
    }

    const Struct& operator[](uint64_t pos) const
    {
        return m_data[pos];
    }

    Struct& operator[](uint64_t pos)
    {
        return m_data[pos];
    }

    uint64_t Size() const
    {
        return m_size;
    }

    template <typename... SoAAttrs>
    void ConvertToSoA(SoA<Attributes<SoAAttrs...>>& dst)
    {
        for (uint64_t i = 0; i < dst.Size(); ++i)
        {
            (memcpy(&dst.SoAAttrs::GetAttribute()[i], &m_data[i].Attrs::GetAttribute(), sizeof(typename Attrs::Type)), ...);
        }
    }

    template <typename Attr, typename AType = typename Attr::Type>
    void SetValues(const AType& value)
    {
        for (uint64_t i = 0; i < m_size; ++i)
        {
            m_data[i].Attr::GetAttribute() = value;
        }
    }

private:
    uint64_t    m_size;
    Struct*     m_data;
};
