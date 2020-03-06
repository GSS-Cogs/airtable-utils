const dataset = window.location.search.substring(1);
if (dataset) {
    Handlebars.registerHelper('ifObject', function(item, options) {
        if (typeof item === "object") {
            return options.fn(this);
        } else {
            return options.inverse(this);
        }
    });
    $.get({
        url: "etl.hbs",
        dataType: "html"
    }, function(source) {
        const template = Handlebars.compile(source);
        $.getJSON('info.json', function(mainInfo) {
            $.getJSON(dataset + "/info.json", function (info) {
                $("#body").html(template({
                    "dataset_path": dataset,
                    "main": mainInfo,
                    "dataset": info,
                    "jenkins_path": mainInfo.jenkins.path.map(function(p) {return 'job/' + p;}).join('/'),
                    "jenkins_buildicon": 'buildStatus/icon?job=' + encodeURIComponent(mainInfo.jenkins.path.join('/') + '/')
                }));
                document.title = info.title;
            });
        });
    });
} else {
    $.get({
        url: "table.hbs",
        datatype: "html",
    }, function(source) {
        const template = Handlebars.compile(source);
        $.getJSON('info.json', function(info) {
            document.title = "Dataset family: " + info.family;
            const fetches = info.pipelines.map(function (pipeline) {
                return $.getJSON(pipeline + '/info.json');
            });
            $.when.apply($, fetches).then(function() {
                const allInfo = arguments;
                const collected = info.pipelines.map(function (pipeline, i) {
                    return {
                        'directory': pipeline,
                        'number': i + 1,
                        'info': allInfo[i][0]
                    };
                });
                $("#body").html(template({
                    "family": info.family,
                    "github": info.github,
                    "jenkins_base": info.jenkins.base,
                    "jenkins_path": info.jenkins.path.map(function(p) {return 'job/' + p;}).join('/'),
                    "jenkins_buildicon": 'buildStatus/icon?job=' + encodeURIComponent(info.jenkins.path.join('/') + '/'),
                    "pipelines": collected
                }));
            });
        });
    });
}
