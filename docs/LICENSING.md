# Licensing: can I use Orch8?

> **Stability: n/a**, legal summary of [LICENSE](../LICENSE).

This page summarizes the [`LICENSE`](../LICENSE) file in this repository. The LICENSE
file is authoritative, and this page isn't legal advice. Where the license text doesn't
give a clear answer, this page says **ask us** instead of guessing. Write to
[hello@orch8.io](mailto:hello@orch8.io).

The Orch8 engine (this repository, "Orch8 Engine") uses the **Business Source License 1.1
(BUSL-1.1)** with an Additional Use Grant. BUSL-1.1 is a source-available license. It
isn't an OSI-approved open-source license until the Change License takes effect.

The SDKs, community templates, and other `orch8-io` repositories carry their own license
files. This page covers only the engine.

## The operative clauses

These are quoted verbatim from [`LICENSE`](../LICENSE).

**Base grant (Terms):**

> The Licensor hereby grants you the right to copy, modify, create derivative works,
> redistribute, and make non-production use of the Licensed Work.

**Additional Use Grant (Parameters):**

> You may make production use of the Licensed Work, provided your use does not include
> offering the Licensed Work to third parties on a hosted or embedded basis that is
> competitive with the Licensor's products.

**Change Date and Change License (Parameters and Terms):**

> Change Date: Four years from the date the Licensed Work is published.
> Change License: Apache License, Version 2.0

> Effective on the Change Date, or the fourth anniversary of the first publicly available
> distribution of a specific version of the Licensed Work under this License, whichever
> comes first, the Licensor hereby grants you rights under the terms of the Change
> License, and the rights granted in the paragraph above terminate.

> This License applies separately for each version of the Licensed Work and the Change
> Date may vary for each version of the Licensed Work released by Licensor.

**If your use doesn't fit:**

> If your use of the Licensed Work does not comply with the requirements currently in
> effect as described in this License, you must purchase a commercial license from the
> Licensor, its affiliated entities, or authorized resellers, or you must refrain from
> using the Licensed Work.

## Can I …?

| Use | Answer | Why (clause) |
|---|---|---|
| Run Orch8 locally, in CI, or in staging to develop and test | **Yes** | Base grant: "make non-production use" |
| Read, copy, and modify the source; keep a private fork | **Yes** | Base grant: "copy, modify, create derivative works" |
| Redistribute original or modified copies | **Yes, under the same license** | Base grant: "redistribute". Also: "All copies of the original and modified Licensed Work, and derivative works … are subject to this License" and "You must conspicuously display this License on each original or modified copy" |
| Run Orch8 in production for your own company's applications and internal workflows | **Yes** | Additional Use Grant: "You may make production use", as long as you don't offer Orch8 itself to third parties in a competing way |
| Self-host Orch8 for your team or company on your own infrastructure or cloud account | **Yes** | Additional Use Grant, same as above |
| Offer Orch8 to third parties as a hosted service (for example a managed workflow or orchestration engine) that competes with Orch8's products, such as Orch8 Cloud | **No. You need a commercial license** | Additional Use Grant excludes "offering the Licensed Work to third parties on a hosted … basis that is competitive with the Licensor's products" |
| Embed Orch8 in a product you ship to third parties where the product competes with Orch8's products | **No. You need a commercial license** | Additional Use Grant excludes offering it "on a … embedded basis that is competitive" |
| Use Orch8 as the internal engine behind your own SaaS, where your customers use your product and never Orch8 directly, and your product isn't a workflow or orchestration offering | **Ask us** | The grant only excludes *competitive* hosted or embedded offerings. The license doesn't define "competitive" or list "the Licensor's products", so we won't guess for you |
| Embed Orch8 in a mobile, desktop, or on-premises product you sell or distribute | **Ask us** | Same undefined "embedded basis that is competitive" test |
| Deploy and operate Orch8 for a client as a consultant or agency | **Ask us** | The license doesn't say whether operating it for a client counts as "offering … to third parties" |
| Use a given version under Apache 2.0 | **Yes, for that version, from its Change Date** | For each version: four years after its first public distribution (or the Change Date, whichever comes first), Apache-2.0 rights replace the BUSL grant. Each version has its own date |
| Use the Orch8 name or logo for your product | **No, not under this license** | "This License does not grant you any right in any trademark or logo of Licensor" |
| Rely on a warranty | **No** | The Licensed Work is "provided on an 'AS IS' basis" |

## Things to know

- **Violations end your rights for every version.** "Any use of the Licensed Work in
  violation of this License will automatically terminate your rights under this License
  for the current and all other versions of the Licensed Work."
- **Third-party copies carry the same terms.** "If you receive the Licensed Work in
  original or modified form from a third party, the terms and conditions set forth in
  this License apply to your use of that work."
- **The container image is labeled BUSL-1.1.** Release images carry
  `org.opencontainers.image.licenses="BUSL-1.1"` and ship the license at
  `/usr/share/licenses/orch8/LICENSE`.
- **Licensor:** Oleksii Vasylenko Tecnologia LTDA. For alternative or commercial
  licensing, including OEM and managed-service use, contact
  [hello@orch8.io](mailto:hello@orch8.io).
