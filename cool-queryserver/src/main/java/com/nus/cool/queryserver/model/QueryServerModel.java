/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.nus.cool.queryserver.model;

import com.nus.cool.core.cohort.ExtendedCohortQuery;
import com.nus.cool.core.cohort.QueryResult;
import com.nus.cool.core.cohort.funnel.FunnelQuery;
import com.nus.cool.core.iceberg.query.IcebergQuery;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.util.config.CsvDataLoaderConfig;
import com.nus.cool.core.util.config.DataLoaderConfig;
import com.nus.cool.core.util.writer.DataWriter;
import com.nus.cool.core.util.writer.ListDataWriter;
import com.nus.cool.extension.util.config.ArrowIPCFileDataLoaderConfig;
import com.nus.cool.extension.util.config.AvroDataLoaderConfig;
import com.nus.cool.extension.util.config.ParquetDataLoaderConfig;
import com.nus.cool.loader.LoadQuery;
import com.nus.cool.model.CoolCohortEngine;
import com.nus.cool.model.CoolLoader;
import com.nus.cool.model.CoolModel;
import com.nus.cool.result.ExtendedResultTuple;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryServerModel {
    private CoolModel coolModel;

    private final CoolCohortEngine cohortEngine = new CoolCohortEngine();

    public QueryServerModel(String datasetPath){
        try{
            this.coolModel = new CoolModel(datasetPath);
        } catch (IOException e){
            System.out.println(e);
        }
    }

    /**
     * Load a new cube
     * @param q query instance
     * @return Response
     */
    public ResponseEntity<String> loadCube(LoadQuery q) {
        try {
            q.isValid();
            String fileType = q.getDataFileType().toUpperCase();
            DataLoaderConfig config;
            switch (fileType){
                case "CSV":
                    config = new CsvDataLoaderConfig();
                    break;
                case "PARQUET":
                    config = new ParquetDataLoaderConfig();
                    break;
                case "ARROW":
                    config = new ArrowIPCFileDataLoaderConfig();
                    break;
                case "AVRO":
                    config = new AvroDataLoaderConfig(new File(q.getConfigPath()));
                    break;
                default:
                    throw new IllegalArgumentException("[x] Invalid load file type: " + fileType);
            }
            System.out.println(config.getClass().getName());
            CoolLoader coolLoader = new CoolLoader(config);
            coolLoader.load(q.getCubeName(),q.getSchemaPath(),q.getDimPath(),q.getDataPath(),q.getOutputPath());
            String resStr = "Cube " + q.getCubeName() + " has already been loaded.";
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(resStr);
        } catch (IOException e){
            System.out.println(e);
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(e.getMessage());
        }
    }

    /**
     * Reload a cublet
     * @param cube the cube to reload
     * @return response
     */
    public ResponseEntity<String> reloadCube(String cube){
        try{
            String resStr;
            if (!this.coolModel.isCubeLoaded(cube)){
                this.coolModel.reload(cube);
                resStr = "Cube " + cube + " is reloaded.";
            } else resStr = "Cube " + cube + " is reloaded.";

            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(resStr);
        } catch (IOException e){
            System.out.println(e);
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(e.getMessage());
        }
    }

    /**
     * List all existing cubes
     * @return cubes
     */
    public ResponseEntity<String[]> listCubes() {
        return ResponseEntity.badRequest()
                .headers(HttpHeaders.EMPTY)
                .body(this.coolModel.listCubes());
    }

    /**
     * Perform CohortSelection
     * @param query user selection query
     * @return query result
     */
    public ResponseEntity<List<String>> cohortSelection(ExtendedCohortQuery query){
        List<String> result = new ArrayList<>();
        try {
            String inputSource = query.getDataSource();
            this.reloadCube(inputSource);
            CubeRS inputCube = this.coolModel.getCube(inputSource);
            List<Integer> users = cohortEngine.selectCohortUsers(inputCube, null, query);
            String outputCohort = query.getOutputCohort();
            File cohortRoot =  new File(coolModel.getCubeStorePath(inputSource), "cohort");
            if(!cohortRoot.exists()){
                cohortRoot.mkdir();
                System.out.println("[*] Cohort Fold " + cohortRoot.getName() + " is created.");
            }
            File cohortFile = new File(cohortRoot, outputCohort);
            if (cohortFile.exists()){
                cohortFile.delete();
                System.out.println("[*] Cohort " + outputCohort + " exists and is deleted!");
            }
            cohortEngine.createCohort(query, users, cohortRoot);
            for(Integer ele: users){
                result.add(ele.toString());
            }
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(result);
        } catch (IOException e){
            System.out.println(e);
            result.add(e.getMessage());
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(result);
        }
    }

    /**
     * List all existing cohorts
     * @return cubes
     */
    public ResponseEntity<String[]> listCohorts(String cube) {
        String[] cohorts = new String[1];
        try {
            cohorts = this.coolModel.listCohorts(cube);
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(cohorts);
        } catch (IOException e){
            System.out.println(e);
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(cohorts);
        }
    }

    /**
     * Execute query Analysis
     * @param query query file
     * @return result
     */
    public ResponseEntity<List<String>> cohortAnalysis(ExtendedCohortQuery query){
        List<String> result = new ArrayList<>();

        try {
            if (!query.isValid())
                throw new IOException("[x] Invalid cohort query.");

            String inputSource = query.getDataSource();
            this.coolModel.reload(inputSource);
            CubeRS inputCube = this.coolModel.getCube(inputSource);
            String inputCohort = query.getInputCohort();
            if (inputCohort != null) {
                System.out.println("Input cohort: " + inputCohort);
                this.coolModel.loadCohorts(inputCohort, inputSource);
            }
            InputVector userVector = this.coolModel.getCohortUsers(inputCohort);
            List<ExtendedResultTuple> results = cohortEngine.performCohortQuery(inputCube, userVector, query);
            System.out.println("Result for the query is  " + results);

            for (ExtendedResultTuple ele: results){
                result.add(ele.toString());
            }

            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(result);

        } catch (IOException e){
            System.out.println(e);
            result.add(e.getMessage());
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(result);
        }
    }

    /**
     * Funnel Analysis
     * @param query query
     * @return Funnel result
     */
    public ResponseEntity<String> funnelAnalysis(FunnelQuery query){
        try {
            if (!query.isValid())
                throw new IOException("[x] Invalid cohort query.");

            String inputSource = query.getDataSource();
            this.coolModel.reload(inputSource);

            CubeRS inputCube = coolModel.getCube(query.getDataSource());
            String inputCohort = query.getInputCohort();
            if (inputCohort != null) {
                System.out.println("Input cohort: " + inputCohort);
                coolModel.loadCohorts(inputCohort, inputSource);
            }
            InputVector userVector = coolModel.getCohortUsers(inputCohort);
            int[] results = coolModel.cohortEngine.performFunnelQuery(inputCube, userVector, query);
            System.out.println("Result for the query is  " + Arrays.toString(results));
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(Arrays.toString(results));

        } catch (IOException e){
            System.out.println(e);
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(e.getMessage());
        }
    }

    /**
     * cohortExploration
     * @param cube cube
     * @param cohort cohort
     * @return result
     */
    public ResponseEntity<List<String>> cohortExploration(String cube, String cohort) {
        List<String> result = new ArrayList<>();

        try{
            // load cube
            coolModel.reload(cube);
            CubeRS inputCube = coolModel.getCube(cube);

            // load cohort
            coolModel.loadCohorts(cohort, cube);
            InputVector userVector = coolModel.getCohortUsers(cohort);

            // export cohort
            List<String> results = new ArrayList<String>();
            DataWriter writer = new ListDataWriter(results);
            coolModel.cohortEngine.exportCohort(inputCube, userVector, writer);

            coolModel.close();
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(result);

        }catch (IOException e){
            System.out.println(e);
            result.add(e.getMessage());
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(result);
        }

    }


    /**
     * Perform iceBergQuery.
     * @param query query
     * @return result
     */
    public ResponseEntity<String> precessIcebergQuery(IcebergQuery query) {
        try{
            String inputSource = query.getDataSource();
            this.coolModel.reload(inputSource);

            QueryResult result;

            List<BaseResult> results = coolModel.olapEngine.performOlapQuery(coolModel.getCube(inputSource), query);
            result = QueryResult.ok(results);

            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(result.toString());

        } catch (Exception e){
            System.out.println(e);
            return ResponseEntity.badRequest()
                    .headers(HttpHeaders.EMPTY)
                    .body(e.getMessage());
        }
    }
}
