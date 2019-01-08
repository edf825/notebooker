$(document).ready(function() {
    $(activeTab).addClass('active').show();
    $('#runReport').click(function(){
        $('#runReportSidebar').sidebar('toggle');
    })
});
