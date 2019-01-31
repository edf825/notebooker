$(document).ready(function() {

    let sb = $('.ui.left.sidebar');
    sb.sidebar({
    transition: 'overlay'
    });
    sb.sidebar('attach events', '#runReport');
    $('.ui .dropdown').dropdown();
});
