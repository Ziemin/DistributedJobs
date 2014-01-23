#ifndef PIPELINES_HPP
#define PIPELINES_HPP

#include "executor.hpp"

namespace dj {

    template <typename Input, typename Reducer>
        struct pipeline {

            Input& input;
            Reducer& reducer;

        };

    template <typename Input>
        struct pipeline<Input> {

        };

    class context {

        public:
            pipeline& pipe_structure() {
                return _pipe;
            }

        private:
            pipeline& _pipe;
    };

}

#endif
