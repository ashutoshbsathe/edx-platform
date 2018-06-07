import logging
import numpy as np
from scipy import stats
from rest_framework import status
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

from edxval.api import get_videos_for_course
from opaque_keys.edx.keys import CourseKey
from openedx.core.djangoapps.request_cache.middleware import request_cached
from openedx.core.lib.api.view_utils import DeveloperErrorViewMixin, view_auth_classes
from openedx.core.lib.graph_traversals import traverse_pre_order
from student.auth import has_course_author_access
from xmodule.modulestore.django import modulestore

from .utils import get_bool_param

log = logging.getLogger(__name__)


@view_auth_classes()
class CourseQualityView(DeveloperErrorViewMixin, GenericAPIView):
    """
    **Use Case**

    **Example Requests**

        GET /api/courses/v1/quality/{course_id}/

    **GET Parameters**

        A GET request may include the following parameters.

        * all
        * sections
        * subsections
        * units
        * videos
        * exclude_graded (boolean) - whether to exclude graded subsections in the subsections and units information.

    **GET Response Values**

        The HTTP 200 response has the following values.

        * is_self_paced - whether the course is self-paced.
        * sections
            * total_number - number of sections in the course.
            * total_visible - number of sections visible to learners in the course.
            * number_with_highlights - number of sections that have at least one highlight entered.
            * highlights_enabled - whether highlights are enabled in the course.
        * subsections
            * total_visible - number of subsections visible to learners in the course.
            * num_with_one_block_type - number of visible subsections containing only one type of block.
            * num_block_types - statistics for number of block types across all visible subsections.
                * min
                * max
                * mean
                * median
                * mode
        * units
            * total_visible - number of units visible to learners in the course.
            * num_blocks - statistics for number of block across all visible units.
                * min
                * max
                * mean
                * median
                * mode
        * videos
            * total_number - number of video blocks in the course.
            * num_with_val_id - number of video blocks that include video pipeline IDs.
            * num_mobile_encoded - number of videos encoded through the video pipeline.
            * durations - statistics for video duration across all videos encoded through the video pipeline.
                * min
                * max
                * mean
                * median
                * mode

    """
    def get(self, request, course_id):
        """
        Returns validation information for the given course.
        """
        all_requested = get_bool_param(request, 'all', False)

        course_key = CourseKey.from_string(course_id)
        if not has_course_author_access(request.user, course_key):
            return self.make_error_response(
                status_code=status.HTTP_403_FORBIDDEN,
                developer_message='The user requested does not have the required permissions.',
                error_code='user_mismatch'
            )
        store = modulestore()
        with store.bulk_operations(course_key):
            course = store.get_course(course_key, depth=self._required_course_depth(request, all_requested))

            response = dict(
                is_self_paced=course.self_paced,
            )
            if get_bool_param(request, 'sections', all_requested):
                response.update(
                    sections=self._sections_quality(course)
                )
            if get_bool_param(request, 'subsections', all_requested):
                response.update(
                    subsections=self._subsections_quality(course, request)
                )
            if get_bool_param(request, 'units', all_requested):
                response.update(
                    units=self._units_quality(course, request)
                )
            if get_bool_param(request, 'videos', all_requested):
                response.update(
                    videos=self._videos_quality(course)
                )

        return Response(response)

    def _required_course_depth(self, request, all_requested):
        if get_bool_param(request, 'units', all_requested):
            return None
        elif get_bool_param(request, 'subsections', all_requested):
            return None
        elif get_bool_param(request, 'sections', all_requested):
            return 1
        else:
            return 0

    def _sections_quality(self, course):
        sections, visible_sections = self._get_sections(course)
        sections_with_highlights = filter(lambda s: s.highlights, visible_sections)
        return dict(
            total_number=len(sections),
            total_visible=len(visible_sections),
            number_with_highlights=len(sections_with_highlights),
            highlights_enabled=course.highlights_enabled_for_messaging,
        )

    def _subsections_quality(self, course, request):
        subsection_unit_dict = self._get_subsections_and_units(course, request)
        num_block_types_per_subsection_dict = {}
        for subsection_key, unit_dict in subsection_unit_dict.iteritems():
            leaf_block_types_in_subsection = (
                unit_info['leaf_block_types']
                for unit_info in unit_dict.itervalues()
            )
            num_block_types_per_subsection_dict[subsection_key] = len(set().union(*leaf_block_types_in_subsection))

        return dict(
            total_visible=len(num_block_types_per_subsection_dict),
            num_with_one_block_type=len(filter(lambda s: s == 1, num_block_types_per_subsection_dict.itervalues())),
            num_block_types=self._stats_dict(list(num_block_types_per_subsection_dict.itervalues())),
        )

    def _units_quality(self, course, request):
        subsection_unit_dict = self._get_subsections_and_units(course, request)
        num_leaf_blocks_per_unit = [
            unit_info['num_leaf_blocks']
            for unit_dict in subsection_unit_dict.itervalues()
            for unit_info in unit_dict.itervalues()
        ]
        return dict(
            total_visible=len(num_leaf_blocks_per_unit),
            num_blocks=self._stats_dict(num_leaf_blocks_per_unit),
        )

    def _videos_quality(self, course):
        video_blocks_in_course = modulestore().get_items(course.id, qualifiers={'category': 'video'})
        videos_in_val = list(get_videos_for_course(course.id))
        video_durations = [video['duration'] for video in videos_in_val]

        return dict(
            total_number=len(video_blocks_in_course),
            num_mobile_encoded=len(videos_in_val),
            num_with_val_id=len(filter(lambda v: v.edx_video_id, video_blocks_in_course)),
            durations=self._stats_dict(video_durations),
        )

    @request_cached
    def _get_subsections_and_units(self, course, request):
        """
        Returns {subsection_key: {unit_key: {num_leaf_blocks: <>, leaf_block_types: set(<>) }}}
        for all visible subsections and units.
        """
        _, visible_sections = self._get_sections(course)
        subsection_dict = {}
        for section in visible_sections:
            visible_subsections = self._get_visible_children(section)

            if get_bool_param(request, 'exclude_graded', False):
                visible_subsections = filter(lambda s: not s.graded, visible_subsections)

            for subsection in visible_subsections:
                unit_dict = {}
                visible_units = self._get_visible_children(subsection)

                for unit in visible_units:
                    leaf_blocks = self._get_leaf_blocks(unit)
                    unit_dict[unit.location] = dict(
                        num_leaf_blocks=len(leaf_blocks),
                        leaf_block_types=set(block.location.block_type for block in leaf_blocks),
                    )

                subsection_dict[subsection.location] = unit_dict
        return subsection_dict

    @request_cached
    def _get_sections(self, course):
        return self._get_all_children(course)

    def _get_all_children(self, parent):
        store = modulestore()
        children = [store.get_item(child_usage_key) for child_usage_key in self._get_children(parent)]
        visible_children = filter(
            lambda s: not s.visible_to_staff_only and not s.hide_from_toc,
            children,
        )
        return children, visible_children

    def _get_visible_children(self, parent):
        _, visible_chidren = self._get_all_children(parent)
        return visible_chidren

    def _get_children(self, parent):
        if not hasattr(parent, 'children'):
            return []
        else:
            return parent.children

    def _get_leaf_blocks(self, unit):
        return [
            block for block in
            traverse_pre_order(unit, self._get_visible_children, lambda b: len(self._get_children(b)) == 0)
        ]

    def _stats_dict(self, data):
        if not data:
            return dict(
                min=None,
                max=None,
                mean=None,
                median=None,
                mode=None,
            )
        else:
            return dict(
                min=min(data),
                max=max(data),
                mean=np.around(np.mean(data)),
                median=np.around(np.median(data)),
                mode=stats.mode(data, axis=None)[0][0],
            )