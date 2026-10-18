# 3. Holds for open orders

Status: accepted

## Context

An open order must not be able to spend cash or shares that another order
or a withdrawal also counts on. Orders can fill in parts, and fills can run
at the same time as cancels and expiry.

## Decision

Accepting an order places a hold: shares for a sell, and for a buy enough
cash at the limit price, or at the ask plus a 2% collar for a market order,
plus the commission, rounded up to the cent. Available balance is balance
less held, and every spend checks it under the row lock. The order keeps
the commission it was accepted with, so changing the commission can't leave
its hold short.

Each fill takes its cost out of the hold with a conditional update, so
concurrent fills queue on the hold and can't use more than is left. What's
left is released when the order fills, is canceled or expires. The hold is
placed in a savepoint inside the order's transaction: if the account can't
cover it, the savepoint is rolled back and the order is stored as rejected
with the reason.

## Alternatives

- Checking buying power only when an order fills. Two open orders could
  each look affordable and together overspend.
- Holding exactly the ask for a market buy. That only works while a market
  order fills at once at the ask its hold was sized at, as it does here; a
  fill at a later, higher ask would find the hold short.

## Consequences

Customers see less available than their balance while orders are open, and
a market buy briefly holds 2% more than it costs. A resting order whose
fill can't be booked, for a reason that won't go away, is canceled with
that reason and its hold released, rather than tried at every quote.
