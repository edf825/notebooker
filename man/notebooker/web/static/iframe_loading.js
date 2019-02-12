var resizeIframe = function (obj) {
    obj.style.height = obj.contentWindow.document.body.scrollHeight + 'px';
    obj.style.width = obj.contentWindow.document.body.scrollWidth + 'px';
};

var iframeLoaded = function(iframe) {
    $(".iframeToLoad").show();
    resizeIframe(iframe);
    $('.iframeLoadingDimmer').removeClass('active').addClass('disabled');

};
