$(document).ready(function() {

    $('#runCheck').click(function () {
        $.ajax({
            type: "POST",
            url: "/run_checks",
            data: $('#run-check-form').serialize(),
            success: function (data, status, request) {
                if (data.status === "Failed") {
                    window.alert(data.error);
                }
                else {
                    console.log(data, status, request);
                    window.location.href = 'task_loading/' + data.id;
                }
            }
        });
    });
});
