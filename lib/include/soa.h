#pragma once

#include <cstdint>

#include "forward.h"

// ------------------------------------------------------------------------------------------

#define SoA_BEGIN(NAME)     \
    namespace NAME          \
    {

#define SoA_END(NAME)       \
    }

#define SoA_ATTRIBUTE(ATTR_NAME)                    \
    template <typename T>                           \
    class ATTR_NAME ## _Attribute                   \
    {                                               \
    public:                                         \
        T*  ATTR_NAME{};                            \
                                                    \
    public:                                         \
        inline constexpr T* GetAttribute()          \
        {                                           \
            return ATTR_NAME;                       \
        }                                           \
                                                    \
    public:                                         \
        using Type = T;                             \
                                                    \
    protected:                                      \
        void SetBuffer(T* buf)                      \
        {                                           \
            ATTR_NAME = buf;                        \
        }                                           \
    };                                              \
                                                    \
    template <typename T>                           \
    using ATTR_NAME = ATTR_NAME ## _Attribute<T>;

// ------------------------------------------------------------------------------------------

template <typename... Attrs>
class SoA<Attributes<Attrs...>>: public Attrs...
{
private:
    constexpr static int SUM_TYPES_SIZE = (0 + ... + sizeof(typename Attrs::Type));

public:
    SoA(uint64_t size)
        : m_size(size)
        , m_data(new unsigned char[SUM_TYPES_SIZE * m_size]{})
    {
        uint64_t sizeSum = 0;
        (
            [this, &sizeSum] {
                using AttrType = typename Attrs::Type;
                this->Attrs::SetBuffer(reinterpret_cast<AttrType*>(this->m_data + sizeSum));
                sizeSum += this->m_size * sizeof(AttrType);
            } (),
            ...
        );
    }

    SoA(const SoA&) = delete;
    SoA& operator=(const SoA&) = delete;
    SoA(SoA&&) = delete;
    SoA& operator=(SoA&&) = delete;

    ~SoA()
    {
        delete[] m_data;
    }

    uint64_t Size() const
    {
        return m_size;
    }

    template <typename... AoSAttrs>
    void ConvertToAoS(AoS<Attributes<AoSAttrs...>>& dst)
    {
        for (uint64_t i = 0; i < dst.Size(); ++i)
        {
            (memcpy(&dst[i].AoSAttrs::GetAttribute(), &Attrs::GetAttribute()[i], sizeof(typename Attrs::Type)), ...);
        }
    }

    template <typename Attr, typename AType = typename Attr::Type>
    void SetValues(const AType& value)
    {
        for (uint64_t i = 0; i < m_size; ++i)
        {
            Attr::GetAttribute()[i] = value;
        }
    }

private:
    uint64_t        m_size;
    unsigned char*  m_data;
};
