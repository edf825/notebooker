$(document).ready(function() {

    $('#run-check-form').submit(function (e) {
        let form = $(this);
        $('#runReportButton').removeClass('active').addClass('disabled');
        $('.message').hide();
        $('#parametersDimmer').show();
        var reportName = $('input[name="report_name"]').val();
        $.ajax({
            type: "POST",
            url: "/run_report/" + reportName,
            data: form.serialize(),
            success: function (data, status, request) {
                if (data.status === "Failed") {
                    $('#errorMsg').text(data.content);
                    $('#errorPopup').show();
                    $('#runReportButton').removeClass('disabled').addClass('active');
                    $('#parametersDimmer').hide();
                }
                else {
                    console.log(data, status, request);
                    window.location.href = '/results/' + reportName + '/' + data.id;
                }
            },
            error: function(jqXHR, textStatus, errorThrown){
                $('#errorMsg').text(jqXHR.status + ' ' + textStatus + ' ' + errorThrown);
                $('#errorPopup').show();
                console.log(jqXHR);
                console.log(textStatus);
                console.log(errorThrown);
                $('#runReportButton').removeClass('disabled').addClass('active');
                $('#parametersDimmer').hide();
            }
        });
        return false;
    });
});
