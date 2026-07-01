package com.vitorpamplona.sot.relay

import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.CountCmd
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.EventCmd
import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.ReqCmd
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.FullAuthPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyResult

/**
 * NIP-42 **optional** auth: the relay offers an AUTH challenge on connect (so a
 * client *can* authenticate and get personalized, WoT-ranked results), but
 * REQ/COUNT/EVENT are allowed anonymously (ranked by the default observer).
 *
 * Inherits the challenge + AUTH-validation from [FullAuthPolicy] and only relaxes
 * the data-command gates from "auth-required" to pass-through.
 */
class OptionalAuthPolicy(relay: NormalizedRelayUrl) : FullAuthPolicy(relay) {
    override fun accept(cmd: ReqCmd): PolicyResult<ReqCmd> = PolicyResult.Accepted(cmd)

    override fun accept(cmd: EventCmd): PolicyResult<EventCmd> = PolicyResult.Accepted(cmd)

    override fun accept(cmd: CountCmd): PolicyResult<CountCmd> = PolicyResult.Accepted(cmd)
}
