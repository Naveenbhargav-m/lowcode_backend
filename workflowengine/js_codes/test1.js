


function ProcessTemplate() {
    let template = inputs["template"];
    let copy = { ...inputs };
    delete copy["template"];

    const result = template.replace(/{{\s*([\w.]+)\s*}}/g, (match, key) => {
        return key in copy ? copy[key] : match;
    });

    return result;
}

ProcessTemplate();