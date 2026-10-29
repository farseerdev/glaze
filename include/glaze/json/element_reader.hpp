// Glaze Library
// For the license information refer to glaze.hpp

#pragma once

#include "glaze/json/read.hpp"

namespace glz
{
   // Reads a JSON array element by element through a callback: `f(index, ctx, it, end)`
   // is called at each element, which it must read or skip, leaving `it` past it. The
   // array's own syntax (brackets, commas, whitespace) and a streaming read's refills
   // between elements are glaze's, as for any array; `f` may refill too, and the read
   // carries on from where `f` leaves the buffer. What `f` throws reaches the caller of
   // the read.
   template <class F>
   struct element_reader
   {
      F f;
   };

   namespace detail
   {
      template <class F>
      struct element_slot
      {
         F& f;
         size_t index;
      };

      // An array that holds no element: glaze's array read appends a slot per element
      // and reads the element through it.
      template <class F>
      struct element_reader_array
      {
         using value_type = element_slot<F>;
         using reference = element_slot<F>;

         F* f;
         size_t count{};

         reference emplace_back() noexcept { return {*f, count++}; }

         const value_type* begin() const noexcept { return nullptr; }
         const value_type* end() const noexcept { return nullptr; }
         size_t size() const noexcept { return 0; }
      };
   }

   template <class F>
   struct from<JSON, element_reader<F>>
   {
      template <auto Opts>
      static void op(auto&& value, is_context auto&& ctx, auto&& it, auto&& end)
      {
         detail::element_reader_array<F> array{&value.f};
         parse<JSON>::op<Opts>(array, ctx, it, end);
      }
   };

   template <class F>
   struct from<JSON, detail::element_slot<F>>
   {
      template <auto Opts>
      static void op(auto&& slot, is_context auto&& ctx, auto&& it, auto end)
      {
         slot.f(slot.index, ctx, it, end);
      }
   };
}
