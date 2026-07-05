/*
 * Copyright (c) 2026 Vitor Pamplona
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.vitorpamplona.sot.sync

import com.vitorpamplona.quartz.experimental.agora.FundraiserEvent
import com.vitorpamplona.quartz.experimental.audio.track.AudioTrackEvent
import com.vitorpamplona.quartz.experimental.fitness.workout.ExerciseTemplateEvent
import com.vitorpamplona.quartz.experimental.fitness.workout.WorkoutRecordEvent
import com.vitorpamplona.quartz.experimental.music.playlist.MusicPlaylistEvent
import com.vitorpamplona.quartz.experimental.music.track.MusicTrackEvent
import com.vitorpamplona.quartz.experimental.nip82SoftwareApps.application.SoftwareApplicationEvent
import com.vitorpamplona.quartz.experimental.nipsOnNostr.NipTextEvent
import com.vitorpamplona.quartz.feedDefinition.FeedDefinitionEvent
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip23LongContent.LongTextNoteEvent
import com.vitorpamplona.quartz.nip28PublicChat.admin.ChannelCreateEvent
import com.vitorpamplona.quartz.nip28PublicChat.admin.ChannelMetadataEvent
import com.vitorpamplona.quartz.nip29RelayGroups.metadata.GroupMetadataEvent
import com.vitorpamplona.quartz.nip30CustomEmoji.pack.EmojiPackEvent
import com.vitorpamplona.quartz.nip34Git.issue.GitIssueEvent
import com.vitorpamplona.quartz.nip34Git.pr.GitPullRequestEvent
import com.vitorpamplona.quartz.nip34Git.repository.GitRepositoryEvent
import com.vitorpamplona.quartz.nip35Torrents.TorrentEvent
import com.vitorpamplona.quartz.nip51Lists.appCurationSet.AppCurationSetEvent
import com.vitorpamplona.quartz.nip51Lists.articleCurationSet.ArticleCurationSetEvent
import com.vitorpamplona.quartz.nip51Lists.bookmarkList.BookmarkListEvent
import com.vitorpamplona.quartz.nip51Lists.bookmarkList.OldBookmarkListEvent
import com.vitorpamplona.quartz.nip51Lists.followList.FollowListEvent
import com.vitorpamplona.quartz.nip51Lists.interestSet.InterestSetEvent
import com.vitorpamplona.quartz.nip51Lists.labeledBookmarkList.LabeledBookmarkListEvent
import com.vitorpamplona.quartz.nip51Lists.mediaStarterPack.MediaStarterPackEvent
import com.vitorpamplona.quartz.nip51Lists.peopleList.PeopleListEvent
import com.vitorpamplona.quartz.nip51Lists.pictureCurationSet.PictureCurationSetEvent
import com.vitorpamplona.quartz.nip51Lists.relaySets.RelaySetEvent
import com.vitorpamplona.quartz.nip51Lists.releaseArtifactSet.ReleaseArtifactSetEvent
import com.vitorpamplona.quartz.nip51Lists.videoCurationSet.VideoCurationSetEvent
import com.vitorpamplona.quartz.nip52Calendar.appt.day.CalendarDateSlotEvent
import com.vitorpamplona.quartz.nip52Calendar.appt.time.CalendarTimeSlotEvent
import com.vitorpamplona.quartz.nip52Calendar.calendar.CalendarEvent
import com.vitorpamplona.quartz.nip53LiveActivities.clip.LiveActivitiesClipEvent
import com.vitorpamplona.quartz.nip53LiveActivities.meetingSpaces.MeetingRoomEvent
import com.vitorpamplona.quartz.nip53LiveActivities.meetingSpaces.MeetingSpaceEvent
import com.vitorpamplona.quartz.nip53LiveActivities.streaming.LiveActivitiesEvent
import com.vitorpamplona.quartz.nip54Wiki.WikiNoteEvent
import com.vitorpamplona.quartz.nip58Badges.definition.BadgeDefinitionEvent
import com.vitorpamplona.quartz.nip5aStaticWebsites.NamedSiteEvent
import com.vitorpamplona.quartz.nip5aStaticWebsites.RootSiteEvent
import com.vitorpamplona.quartz.nip5dNapplets.NamedNappletEvent
import com.vitorpamplona.quartz.nip5dNapplets.NappletSnapshotEvent
import com.vitorpamplona.quartz.nip5dNapplets.RootNappletEvent
import com.vitorpamplona.quartz.nip68Picture.PictureEvent
import com.vitorpamplona.quartz.nip71Video.VideoHorizontalEvent
import com.vitorpamplona.quartz.nip71Video.VideoNormalEvent
import com.vitorpamplona.quartz.nip71Video.VideoShortEvent
import com.vitorpamplona.quartz.nip71Video.VideoVerticalEvent
import com.vitorpamplona.quartz.nip72ModCommunities.definition.CommunityDefinitionEvent
import com.vitorpamplona.quartz.nip75ZapGoals.GoalEvent
import com.vitorpamplona.quartz.nip7DThreads.ThreadEvent
import com.vitorpamplona.quartz.nip84Highlights.HighlightEvent
import com.vitorpamplona.quartz.nip89AppHandlers.definition.AppDefinitionEvent
import com.vitorpamplona.quartz.nip94FileMetadata.FileHeaderEvent
import com.vitorpamplona.quartz.nip99Classifieds.ClassifiedsEvent
import com.vitorpamplona.quartz.nipB0WebBookmarks.WebBookmarkEvent
import com.vitorpamplona.quartz.nipC0CodeSnippets.CodeSnippetEvent
import com.vitorpamplona.quartz.nipF4Podcasts.episode.PodcastEpisodeEvent
import com.vitorpamplona.quartz.nipF4Podcasts.metadata.PodcastMetadataEvent

/**
 * The kinds the store can index for NIP-50 search, and therefore the kinds the
 * records plane ([BlendedPass]'s content units) pulls for scored authors.
 *
 * This is the SAME set the store's `SearchExtractors` decomposes — one entry per
 * explicit extractor branch — so what we sync equals what we can index. The list
 * is kept in lockstep with `SearchExtractors` by hand (Quartz's `EventFactory`
 * registry is populated lazily and can't be enumerated cold, so there's no safe
 * runtime derivation); `IndexableKindsTest` guards the staples, and adding a new
 * searchable kind is a one-line entry in both places.
 *
 * The `KIND` constants come straight from Quartz, so the numbers can't drift
 * from the event definitions.
 */
object IndexableKinds {
    /** Every searchable kind, ascending and distinct. Deletions (5/62) are NOT here — the caller adds them. */
    val kinds: List<Int> =
        listOf(
            MetadataEvent.KIND,
            TextNoteEvent.KIND,
            LongTextNoteEvent.KIND,
            WikiNoteEvent.KIND,
            ClassifiedsEvent.KIND,
            GitRepositoryEvent.KIND,
            GitIssueEvent.KIND,
            GitPullRequestEvent.KIND,
            CommunityDefinitionEvent.KIND,
            EmojiPackEvent.KIND,
            ChannelCreateEvent.KIND,
            ChannelMetadataEvent.KIND,
            PictureEvent.KIND,
            VideoNormalEvent.KIND,
            VideoHorizontalEvent.KIND,
            VideoVerticalEvent.KIND,
            VideoShortEvent.KIND,
            TorrentEvent.KIND,
            ThreadEvent.KIND,
            FundraiserEvent.KIND,
            NipTextEvent.KIND,
            ExerciseTemplateEvent.KIND,
            WorkoutRecordEvent.KIND,
            CalendarEvent.KIND,
            LiveActivitiesClipEvent.KIND,
            CalendarDateSlotEvent.KIND,
            CalendarTimeSlotEvent.KIND,
            LiveActivitiesEvent.KIND,
            MeetingSpaceEvent.KIND,
            MeetingRoomEvent.KIND,
            CodeSnippetEvent.KIND,
            BadgeDefinitionEvent.KIND,
            MusicPlaylistEvent.KIND,
            MusicTrackEvent.KIND,
            SoftwareApplicationEvent.KIND,
            PodcastEpisodeEvent.KIND,
            PodcastMetadataEvent.KIND,
            GroupMetadataEvent.KIND,
            InterestSetEvent.KIND,
            FollowListEvent.KIND,
            MediaStarterPackEvent.KIND,
            PictureCurationSetEvent.KIND,
            ArticleCurationSetEvent.KIND,
            VideoCurationSetEvent.KIND,
            ReleaseArtifactSetEvent.KIND,
            AppCurationSetEvent.KIND,
            RelaySetEvent.KIND,
            WebBookmarkEvent.KIND,
            NamedSiteEvent.KIND,
            RootSiteEvent.KIND,
            RootNappletEvent.KIND,
            NappletSnapshotEvent.KIND,
            NamedNappletEvent.KIND,
            FeedDefinitionEvent.KIND,
            LabeledBookmarkListEvent.KIND,
            PeopleListEvent.KIND,
            BookmarkListEvent.KIND,
            OldBookmarkListEvent.KIND,
            GoalEvent.KIND,
            HighlightEvent.KIND,
            FileHeaderEvent.KIND,
            AudioTrackEvent.KIND,
            AppDefinitionEvent.KIND,
        ).distinct().sorted()
}
