// Glaze Library
// For the license information refer to glaze.hpp

#pragma once

#include "glaze/json/read.hpp"

namespace glz
{
   // Reads a JSON object member by member through a callback: `f(key, ctx, it, end)`
   // is called at each member's value, which it must read or skip, leaving `it` past
   // it. The key is the raw, still escaped key; in a streaming read it is valid only
   // until the callback refills the buffer. What `f` throws reaches the caller of the
   // read.
   template <class F>
   struct member_reader
   {
      F f;
   };

   namespace detail
   {
      template <class F>
      struct member_slot
      {
         F& f;
         sv key;
      };

      // An object without members whose every key is unknown and goes to `slots`.
      template <class F>
      struct member_reader_root
      {
         struct slots_t
         {
            using key_type = sv;
            F* f;
            member_slot<F> operator[](const sv key) const noexcept { return {*f, key}; }
         };
         slots_t slots;

         struct glaze
         {
            static constexpr auto value = object();
            static constexpr auto unknown_read = &member_reader_root::slots;
         };
      };
   }

   template <class F>
   struct from<JSON, member_reader<F>>
   {
      template <auto Opts>
      static void op(auto&& value, is_context auto&& ctx, auto&& it, auto&& end)
      {
         detail::member_reader_root<F> root{{&value.f}};
         parse<JSON>::op<opt_false<Opts, &opts::error_on_unknown_keys>>(root, ctx, it, end);
      }
   };

   template <class F>
   struct from<JSON, detail::member_slot<F>>
   {
      template <auto Opts>
      static void op(auto&& slot, is_context auto&& ctx, auto&& it, auto end)
      {
         slot.f(slot.key, ctx, it, end);
      }
   };
}
