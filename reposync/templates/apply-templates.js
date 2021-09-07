let dataset = window.location.search.substring(1);
let spec = false;
if (dataset.startsWith('spec=')) {
    spec = true;
    dataset = dataset.substring(5);
}

function datasetFetcher(endpoint, pipelineJobs) {
    let filteredURLs = pipelineJobs.filter(p => {
        try {
            new URL(p);
            return true;
        } catch (err) {
            console.warn(`Invalid Jenkins job page URL "${p}"`);
            return false;
        }
    });
    return $.post({
        url: endpoint,
        data: {
            "query": `PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX pmdcat: <http://publishmydata.com/pmdcat#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX prov: <http://www.w3.org/ns/prov#>
SELECT DISTINCT ?page ?ds ?label ?modified ?pipelineJob ?cubeType WHERE {
  ?jobGraph prov:wasGeneratedBy [ prov:wasAssociatedWith ?pipelineJob ] .
  GRAPH ?jobGraph {
    ?ds a pmdcat:Dataset;
      rdfs:label ?label .
    OPTIONAL {
      ?ds dcat:landingPage ?page
    }
    OPTIONAL {
      ?ds dct:modified ?modified
    }
  }
  ?ds pmdcat:datasetContents [ a qb:DataSet ] .
} VALUES (?pipelineJob) {
${filteredURLs.map(x => `(<${(new URL(x)).href}>)`).join(' ')}
}`
        },
        dataType: 'json',
        headers: {"Accept": "application/sparql-results+json"}
    }).then(function(results) {
        function safeGetValue(res, prop) {
            if (res.hasOwnProperty(prop) && res[prop].hasOwnProperty('value')) {
                return res[prop].value
            }
            return null;
        }
        function asDate(v) {
            return (v !== null) ? new Date(v) : v;
        }
        return results.results.bindings.map(binding => {
            return {
                landingPage: safeGetValue(binding, 'page'),
                uri: safeGetValue(binding, 'ds'),
                label: safeGetValue(binding, 'label'),
                modified: asDate(safeGetValue(binding,'modified')),
                job: safeGetValue(binding, 'pipelineJob')
            };
        });
    });
}

if (dataset) {
    Handlebars.registerHelper('ifObject', function (item, options) {
        if (typeof item === "object") {
            return options.fn(this);
        } else {
            return options.inverse(this);
        }
    });
    if (spec) {
        $.get({url: "spec.hbs", dataType: "html"}, function (source) {
            const template = Handlebars.compile(source);
            $.getJSON('info.json', function (mainInfo) {
                let spec_url = mainInfo.jenkins.base + '/' + mainInfo.jenkins.path.map(function (p) {
                    return 'job/' + p;
                }).join('/') + '/' + dataset + '/lastBuild/artifact/datasets/' + dataset + '/out/spec.json';
                $.getJSON(spec_url, function (spec_obj) {
                    $("#body").html(template({
                        "dataset_path": dataset,
                        "main": mainInfo,
                        "spec": spec_obj
                    }));
                });
            });
        });
    } else {
        $.get({
            url: "etl.hbs",
            dataType: "html"
        }, function (source) {
            const template = Handlebars.compile(source);
            $.getJSON('info.json', function (mainInfo) {
                const pipelinesBase = mainInfo.jenkins.base + '/' + mainInfo.jenkins.path.map(function (i) {
                    return 'job/' + i;
                }).join('/');
                $.getJSON(dataset + "/info.json", function (info) {
                    let fetchDatasets, lastPublished = null;
                    if (mainInfo.hasOwnProperty('sparql')) {
                        fetchDatasets = datasetFetcher(mainInfo.sparql, [pipelinesBase + '/job/' + dataset + '/']);
                    } else {
                        fetchDatasets = $.Deferred();
                        fetchDatasets.resolve([]);
                    }
                    fetchDatasets.done(function (datasets) {
                        if (mainInfo.hasOwnProperty('pmd')) {
                            datasets = datasets.map(function (ds) {
                                if ((ds.modified !== null) && ((lastPublished === null) || (lastPublished < ds.modified))) {
                                    lastPublished = ds.modified;
                                }
                                return {
                                    pmd: mainInfo.pmd + '/cube/explore?uri=' + encodeURIComponent(ds.uri),
                                    label: ds.label,
                                    modified: ds.modified
                                }
                            });
                        }
                        $("#body").html(template({
                            "dataset_path": dataset,
                            "main": mainInfo,
                            "dataset": info,
                            "jenkins_path": mainInfo.jenkins.path.map(function (p) {
                                return 'job/' + p;
                            }).join('/'),
                            "jenkins_buildicon": 'buildStatus/icon?job=' + encodeURIComponent(mainInfo.jenkins.path.join('/') + '/'),
                            "datasets": datasets,
                            "issue_badge_base": `https://img.shields.io/github/issues/detail/state${(new URL(mainInfo.github)).pathname}`
                        }));
                    });
                    document.title = info.title;
                });
            });
        });
    }
} else {
    Handlebars.registerHelper('rowClass', function(f, i) {
      if (i.hasOwnProperty('families') && !i.families.includes(f)) {
        return 'text-danger';
      } else if (i.hasOwnProperty('extract') && i.extract.hasOwnProperty('stage')) {
        if (i.extract.stage === 'Prioritized') {
          return 'text-body';
        } else {
          return 'text-muted';
        }
      } else {
        return 'text-body';
      }
    });
    $.get({
        url: "table.hbs",
        datatype: "html",
    }, function(source) {
        const template = Handlebars.compile(source);
        $.getJSON('info.json', function(info) {
            document.title = "Dataset family: " + info.family;
            const fetches = info.pipelines.map(function (pipeline) {
                return $.getJSON(pipeline + '/info.json')
                    .then(function(dsinfo) {
                        return $.ajax({url: pipeline + '/spec.md', method: 'HEAD'})
                            .then(function () {
                                dsinfo.spec = pipeline + '/spec';
                                return dsinfo;
                            }, function() {
                                return $.Deferred().resolve(dsinfo).promise();
                            });
                        }, function() {
                            console.log(`Failed fetching/parsing ${pipeline} info.json`);
                            return $.Deferred().resolve({}).promise();
                        })
                    .then(function(dsinfo) {
                        return $.ajax({url: pipeline + '/flowchart.ttl', method: 'HEAD'})
                            .then(function () {
                                dsinfo.flowchart = 'specflowcharts.html?' + pipeline + '/flowchart.ttl';
                                return dsinfo;
                            }, function() {
                                return $.Deferred().resolve(dsinfo).promise();
                            });
                    })
                    .then(function(dsinfo) {
                        let spec_url = info.jenkins.base + '/' + info.jenkins.path.map(function (p) {
                            return 'job/' + p;
                        }).join('/') + '/' + pipeline + '/lastBuild/artifact/datasets/' + pipeline + '/out/spec.json';
                        return $.ajax({url: spec_url, method: 'HEAD'})
                            .then(function () {
                                dsinfo.jenkinsSpec = '?spec=' + pipeline;
                                return dsinfo;
                            }, function() {
                                return $.Deferred().resolve(dsinfo).promise();
                            });

                    })
            });
            $.when.apply($, fetches).then(function() {
                const allInfo = arguments;
                const pipelinesBase = info.jenkins.base + '/' + info.jenkins.path.map(function (i) {
                    return 'job/' + i;
                }).join('/');
                let collected = info.pipelines.map(function (pipeline, i) {
                    return {
                        'directory': pipeline,
                        'number': i + 1,
                        'job': pipelinesBase + '/job/' + pipeline + '/',
                        'info': allInfo[i]
                    };
                });
                let fetchDatasets;
                if (info.hasOwnProperty('sparql')) {
                    fetchDatasets = datasetFetcher(info.sparql, collected.map(p => p.job));
                } else {
                    fetchDatasets = $.Deferred();
                    fetchDatasets.resolve([]);
                }
                fetchDatasets.done(function(datasets) {
                    collected = collected.map(p => {
                        p.datasets = datasets.filter(ds => (p.job === ds.job));
                        let lastModified = p.datasets
                            .map(a => a.modified)
                            .reduce(function(a, b) {
                                    if (a === null) {
                                        return b;
                                    } else if (b === null) {
                                        return a;
                                    } else {
                                        return a > b ? a : b
                                    }
                                }, null);
                        if (info.hasOwnProperty('pmd')) {
                            p.datasets = p.datasets.map(ds => {
                                ds.pmd = info.pmd + '/cube/explore?uri=' + encodeURIComponent(ds.uri);
                                return ds;
                            });
                        };
                        if (lastModified !== null) {
                          p.lastModified = lastModified.toDateString();
                          p.lastModifiedMillis = lastModified.valueOf();
                        }
                        return p;
                    });
                    $("#body").html(template({
                        "family": info.family,
                        "github": info.github,
                        "githubcogs": info.githubcogs,
                        "jenkins_base": info.jenkins.base,
                        "jenkins_path": info.jenkins.path.map(function (p) {
                            return 'job/' + p;
                        }).join('/'),
                        "jenkins_buildicon": 'buildStatus/icon?job=' + encodeURIComponent(info.jenkins.path.join('/') + '/'),
                        "pipelines": collected,
                        "issue_badge_base": `https://img.shields.io/github/issues/detail/state${(new URL(info.github)).pathname}`
                    }));
                    $.fn.dataTable.ext.search.push(
                        function( settings, data, dataIndex ) {
                            const all = $('#toggle_all').hasClass('active');
                            if (all) return true;
                            let tech_stages = data[4].split(',').map(s => s.trim().toUpperCase());
                            if ((tech_stages.length === 1) && (tech_stages[0] === '')) {
                                tech_stages = [];
                            }
                            let ba_stages = data[3].split(',').map(s => s.trim().toUpperCase());
                            if ((ba_stages.length === 1) && (ba_stages[0] === '')) {
                                ba_stages = [];
                            }
                            return !((ba_stages.length === 0) || (tech_stages.indexOf('HOLD') >= 0) ||
                                (ba_stages.indexOf('NOT REQUIRED') >= 0) ||
                                (ba_stages.indexOf('CANDIDATE') >= 0));
                        }
                    );
                    let table = $('#datasets_table').DataTable({
                      "paging": false
                    });
                    $('#toggle_all').click(function() {
                        $(this).button('toggle');
                        table.draw();
                    });
                });
            });
        });
    });
}
