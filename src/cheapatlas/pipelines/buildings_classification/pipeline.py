# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This is a boilerplate pipeline 'buildings_classification'
generated using Kedro 0.16.6
"""

from kedro.pipeline import Pipeline, node
from src.cheapatlas.pipelines.buildings_classification.nodes import *

def create_pipeline(**kwargs):
    return Pipeline([
        node(
            func=generate_features,
            inputs=['raw_plz_ags',
                    'params:boundary_type',
                    'params:int_buildings_path',
                    'params:pri_buildings_path'],
            outputs=None,
            name='generate_footprint_features'
        ),
        node(
            func=building_block_clustering,
            inputs=['raw_plz_ags',
                    'params:boundary_type',
                    'params:pri_buildings_path',
                    'params:fea_buildings_path'],
            outputs=None,
            name='building_block_clustering'
        ),
        node(
            func=building_types_classification,
            inputs=['raw_plz_ags',
                    'params:boundary_type',
                    'params:fea_buildings_path',
                    'params:model_output_path'],
            outputs=None,
            name='building_type_classification'
        )
    ], tags="buildings_classification_pipeline"
    )

