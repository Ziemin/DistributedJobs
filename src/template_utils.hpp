#ifndef TEMPLATE_UTILS_HPP
#define TEMPLATE_UTILS_HPP

#include <type_traits>
#include <functional>

namespace dj {

    // is_any_name struct lets you know in compilation time if any of the variadic 
    // template parameters equals first parameter
    template <typename T, typename...>
        struct is_any_same : std::true_type 
    {};

    template <typename T, typename V, typename... Types>
        struct is_any_same<T, V, Types...>
        : std::integral_constant<bool, std::is_same<T, V>{} || is_any_same<T, Types...>{}>
        {};


    // Runs a functor on every template parameter from list and returns true
    // when functor returns true for the first time, otherwise return false
    template<template<typename> class FuncT, typename T, typename... Tp>
        struct for_each_any {

            template <typename... Args>
                static bool run(Args&&... args) {
                    FuncT<T> f;
                    if(f(std::forward<Args>(args)...)) return true;
                    return for_each_any<FuncT, Tp...>::run(std::forward<Args>(args)...);
                }
        };

    template<template<typename> class FuncT, typename T>
        struct for_each_any<FuncT, T> {

            template <typename... Args>
                static bool run(Args&&... args) {
                    FuncT<T> f;
                    return f(std::forward<Args>(args)...);
                }
        };

}

#endif
