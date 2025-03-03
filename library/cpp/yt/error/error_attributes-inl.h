#ifndef ERROR_ATTRIBUTES_INL_H_
#error "Direct inclusion of this file is not allowed, include error_attributes.h"
// For the sake of sane code completion.
#include "error_attributes.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T TErrorAttributes::GetAndRemove(const TString& key)
{
    auto result = Get<T>(key);
    Remove(key);
    return result;
}

template <class T>
T TErrorAttributes::Get(TStringBuf key, const T& defaultValue) const
{
    return Find<T>(key).value_or(defaultValue);
}

template <class T>
T TErrorAttributes::GetAndRemove(const TString& key, const T& defaultValue)
{
    auto result = Find<T>(key);
    if (result) {
        Remove(key);
        return *result;
    } else {
        return defaultValue;
    }
}

template <class T>
typename TOptionalTraits<T>::TOptional TErrorAttributes::FindAndRemove(const TString& key)
{
    auto result = Find<T>(key);
    if (result) {
        Remove(key);
    }
    return result;
}

template <CMergeableDictionary TDictionary>
void TErrorAttributes::MergeFrom(const TDictionary& dict)
{
    using TTraits = TMergeDictionariesTraits<TDictionary>;

    for (const auto& [key, value] : TTraits::MakeIterableView(dict)) {
        SetYson(key, value);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <>
struct TMergeDictionariesTraits<TErrorAttributes>
{
    static auto MakeIterableView(const TErrorAttributes& attributes)
    {
        return attributes.ListPairs();
    }
};

static_assert(CMergeableDictionary<TErrorAttributes>);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
