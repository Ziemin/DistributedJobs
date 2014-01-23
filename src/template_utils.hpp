#ifndef TEMPLATE_UTILS_HPP
#define TEMPLATE_UTILS_HPP

#include <type_traits>

// is_any_name struct lets you know in compilation time if any of the variadic 
// template parameters equals first parameter

template <typename T, typename...>
struct is_any_same : std::true_type 
{};

template <typename T, typename V, typename... Types>
struct is_any_same<T, V, Types...>
    : std::integral_constant<bool, std::is_same<T, V>{} || is_any_same<T, Types...>{}>
{};

#endif
