#pragma once

#include <arrow/type.h>
#include <arrow/type_traits.h>
#include <memory>
#include <utility>

#include "pretty_type_traits.h"


static inline bool BaseCompare(const std::shared_ptr<arrow::DataType>& lhs,
                               const std::shared_ptr<arrow::DataType>& rhs) noexcept
{
    bool is_numeric_lhs = arrow::is_numeric(lhs->id());
    bool is_numeric_rhs = arrow::is_numeric(lhs->id());

    if (is_numeric_lhs != is_numeric_rhs)
    {
        return is_numeric_lhs;
    }
    else
    {
        if (is_numeric_lhs)
        {
            if (GetFixedSizeTypeWidth(lhs) != GetFixedSizeTypeWidth(rhs))
            {
                return GetFixedSizeTypeWidth(lhs) < GetFixedSizeTypeWidth(rhs);
            }
            else
            {
                return lhs->id() < rhs->id();
            }
        }
        else
        {
            return lhs->id() < rhs->id();
        }
    }    
}

struct TypesComparator
{
    bool operator()(const std::shared_ptr<arrow::Field>& lhs,
                    const std::shared_ptr<arrow::Field>& rhs) const noexcept
    {
        return BaseCompare(lhs->type(), rhs->type());
    }

    bool neq(const std::shared_ptr<arrow::Field>& lhs,
             const std::shared_ptr<arrow::Field>& rhs) const noexcept
    {
        return BaseCompare(lhs->type(), rhs->type()) || BaseCompare(rhs->type(), lhs->type());
    }
};

struct TypesWithDataComparator
{
private:
    using TypeValuePair = std::pair<std::shared_ptr<arrow::Field>, std::shared_ptr<arrow::Array>>;

public:
    bool operator()(const TypeValuePair& lhs,
                    const TypeValuePair& rhs) const noexcept
    {
        return BaseCompare(lhs.first->type(), rhs.first->type());    
    }
};
